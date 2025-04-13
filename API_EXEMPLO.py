#!/usr/bin/env python
# coding: utf-8

# In[20]:


from fastapi import FastAPI, Request, UploadFile, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os


app = FastAPI()

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def form(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile):
    df_teste = pd.read_excel(file.file)
    df_teste['produto'] = df_teste['produto'].str.split().str[:4].str.join(' ')
    df_teste['data_pedido'] = pd.to_datetime(df_teste['data_pedido'], dayfirst=True)
    df_teste['data_pedido'] = df_teste['data_pedido'].ffill()
    df_teste['nome']=df_teste['nome'].ffill()
    df_teste['email']=df_teste['email'].ffill()
    df_teste['cep_cliente']=df_teste['cep_cliente'].ffill()
    df_teste['endereco_contido'] = df_teste.apply(
    lambda row: str(row['rua_cliente_numero']).lower() in str(row['bairro_cliente']).lower(), axis=1
    )
    # Função para limpar e separar os valores corretamente
    def separate_numbers(valor_unitario):
        if isinstance(valor_unitario, str):
            return [val for val in valor_unitario.split('||') if val.replace('.', '', 1).isdigit()]
        else:
            return [valor_unitario]
    
    # Função para separar os itens de produto
    def separate_item_names(item_name):
        if isinstance(item_name, str):
            return item_name.split('||')  # Separar pela barra dupla
        else:
            return [item_name]
    
    # Separando os valores da coluna 'valor_unitario' e 'produto'
    df_separated_cost = df_teste.set_index('numero_pedido')['valor_unitario'].apply(separate_numbers)
    df_separated_name = df_teste.set_index('numero_pedido')['produto'].apply(separate_item_names)
    df_separated_qtd = df_teste.set_index('numero_pedido')['quantidade'].apply(separate_item_names)
    
    # Ajustar o tamanho das listas para garantir que elas tenham o mesmo comprimento
    max_len = max(df_separated_cost.apply(len).max(), df_separated_name.apply(len).max())
    
    # Preencher as listas menores com None ou valores vazios para garantir o mesmo comprimento
    df_separated_cost = df_separated_cost.apply(lambda x: x + [None] * (max_len - len(x)))
    df_separated_name = df_separated_name.apply(lambda x: x + [None] * (max_len - len(x)))
    
    # Agora, vamos combinar as listas de 'valor_unitario' e 'produto' por 'numero_pedido'
    df_separated = pd.DataFrame({
        'numero_pedido': df_separated_cost.index.repeat(max_len),
        'valor_unitario': [item for sublist in df_separated_cost for item in sublist],
        'produto': [item for sublist in df_separated_name for item in sublist],
        'quantidade': [item for sublist in df_separated_qtd for item in sublist]
    })
    
    # Convertendo 'valor_unitario' para numérico e removendo valores não numéricos
    df_separated['valor_unitario'] = pd.to_numeric(df_separated['valor_unitario'], errors='coerce')
    
    # Remover as linhas com NaN (caso algum valor não tenha sido convertido corretamente)
    df_separated = df_separated.dropna(subset=['valor_unitario'])
    df_separated['Valor_individual'] = df_separated['valor_unitario']*df_separated['quantidade']
    df_separated['Item_individual'] = df_separated['produto']
    df_separated = df_separated[['numero_pedido', 'Item_individual', 'Valor_individual']]
    df_teste1 = df_separated.merge(df_teste, on='numero_pedido', how='left')
    df_teste.loc[df_teste['telefone']=='N/D','telefone']=0
    df_teste['Valor_individual'] = df_teste['quantidade']*df_teste['valor_unitario']
    clientes = df_teste[['nome', 'Valor_individual', 'data_pedido','email']].groupby(['nome', 'data_pedido','email'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).copy()
    faturamento = df_teste[['data_pedido', 'Valor_individual']].groupby(['data_pedido'], as_index=False).sum().sort_values(by='data_pedido', ascending=False).reset_index(drop=True)
    faturamento = df_teste[['data_pedido', 'Valor_individual']].groupby(['data_pedido'], as_index=False).sum().sort_values(by='data_pedido', ascending=False).reset_index(drop=True)
    faturamento['data_pedido'] = pd.to_datetime(faturamento['data_pedido'])
    faturamento['dia_semana'] = faturamento['data_pedido'].dt.strftime('%A')
    faturamento['data_pedido'] = faturamento['data_pedido'].dt.strftime('%Y-%m-%d')
    faturamento['data_pedido'] = pd.to_datetime(faturamento['data_pedido'])
    media_faturamento = faturamento['Valor_individual'].mean()
    faturamento = faturamento.groupby(['data_pedido', 'dia_semana'], as_index=False).sum()
    faturamento['dia_semana'] = faturamento['dia_semana'].replace({
        'Monday': 'segunda-feira',
        'Tuesday': 'terça-feira',
        'Wednesday': 'quarta-feira',
        'Thursday': 'quinta-feira',
        'Friday': 'sexta-feira',
        'Saturday': 'sábado',
        'Sunday': 'domingo'
    })
    fat = faturamento.copy()
    fat['data_pedido'] = pd.to_datetime(fat['data_pedido'].dt.to_period('M').astype(str), errors='coerce')
    faturamento['Ano'] = pd.to_datetime(faturamento['data_pedido'].dt.to_period('Y').astype(str), errors='coerce').dt.date
    df_teste['ano'] = pd.to_datetime(faturamento['data_pedido']).dt.to_period('Y').astype(str)
    df_teste['mes'] = pd.to_datetime(faturamento['data_pedido'], dayfirst=True).dt.to_period('M').astype(str)
    df_teste['mes'] = pd.to_datetime(df_teste['data_pedido'], dayfirst=True).dt.to_period('M').astype(str)
    df_teste['ano'] = pd.to_datetime(df_teste['data_pedido'], dayfirst=True).dt.to_period('Y').astype(str)
    df_teste['Valor_individual'] = df_teste['quantidade']*df_teste['valor_unitario']
    prod = df_teste[['produto', 'Valor_individual', 'quantidade', 'ano', 'mes']].groupby(['produto', 'ano', 'mes'], as_index=False).sum()
    prod['Valor_individual']=prod['Valor_individual']
    df_teste['nome'] = df_teste[['nome', 'sobrenome']].astype(str).agg(' '.join, axis=1)
    df_teste['Cont']=1
    df_sorted = df_teste[['nome', 'produto', 'Cont']].sort_values(by=['nome', 'Cont'], ascending=[True, False])
    def calcular_primeira_combinacao(df):
        relacoes = []
        for cliente, grupo in df_sorted.groupby('nome'):
            # Pegar os 3 primeiros produtos (maior "Cont") para cada cliente
            produtos = grupo.head(3)
            
            if len(produtos) == 3:  # Certificar que o cliente tem pelo menos dois produtos
                produto1 = produtos.iloc[0]
                produto2 = produtos.iloc[1]
                produto3 = produtos.iloc[2]
                
                # Obter os valores de "Cont" para cada produto
                cont1 = produto1['Cont']
                cont2 = produto2['Cont']
                cont3 = produto3['Cont']
                
                # Adicionar a relação ao resultado
                relacoes.append({
                    'Cliente': cliente,
                    'Produto 1': produto1['produto'],
                    'Produto 2': produto2['produto'],
                    'Produto 3': produto3['produto'],
                    'Cont 1': cont1,
                    'Cont 2': cont2,
                    'cont 3': cont3,
                    'Soma Cont': cont1 + cont2 + cont3
                })
        
        # Criar um DataFrame com as relações
        return pd.DataFrame(relacoes)
    
    # Calcular a primeira combinação
    relacoes = calcular_primeira_combinacao(df_sorted)
    
    base_prd=relacoes[(relacoes['Produto 1']!='FraÃ§Ã£o Kit - Queij') & (relacoes['Produto 2']!='CafÃ© Especial O Fla')][['Produto 1', 
                      'Produto 2',
                      'Produto 3',                                                                                                    
                      'Cont 1', 
                      'Cont 2',
                      'cont 3']].groupby(['Produto 1', 
                                          'Produto 2',
                                        'Produto 3'],
                                         as_index=False).sum().sort_values(by=['Cont 1', 
                                                                               'Cont 2',
                                                                              'cont 3'], 
                                                                           ascending=False).drop_duplicates(subset=['Produto 1'], keep='first').head(10)
    
    # # Exibir o resultado
    # relacoes = relacoes[(relacoes['Produto 1']!='FraÃ§Ã£o Kit - Queij') & (relacoes['Produto 2']!='CafÃ© Especial O Fla')][['Produto 1', 
    #                                       'Produto 2',
    #                                       'Produto 3',
    #                                       'Cont 1', 
    #                                        'Cont 2',
    #                                        'cont 3']].groupby(['Produto 1', 
    #                                       'Produto 2'],
    #                                      as_index=False).sum().sort_values(by=['Cont 1', 
    #                                                                            'Cont 2',
    #                                                                           'cont 3'], 
    #                                                                        ascending=False).drop_duplicates(subset=['Produto 1'], keep='first')
    relacoes['Produto 3'] = relacoes['Produto 3'].str.split(' - ').str[0]
    
    clientes1 = clientes[['nome', 'Valor_individual']].reset_index(drop=True)#
    clientes1=clientes1.reset_index(drop=True).iloc[:10]
    clientes1 = clientes[['nome', 'email', 'Valor_individual']].groupby(['nome', 'email'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).head(10)
    clientes1['Valor_individual1'] = clientes1['Valor_individual'].apply(lambda x: f'R${x:,.2f}')
    colors = px.colors.qualitative.Set3
    unique_descriptions = clientes1['nome'].unique()
    num_categories = len(unique_descriptions)
    
    def format_currency(value):
        return "${:,.2f}".format(value)
    
    # Formatar os valores 'Valor_individual_fmt' como dinheiro
    clientes1['Valor_individual_fmt_fmt'] = clientes1['Valor_individual'].apply(format_currency)
    clientes['Valor_individual_fmt_fmt'] = clientes['Valor_individual'].apply(format_currency)
    if num_categories > len(colors):
        colors = px.colors.qualitative.Set2 
    color_map = {desc: colors[i % len(colors)] for i, desc in enumerate(unique_descriptions)}
    bar_colors = [color_map[desc] for desc in clientes1['nome']]
    
    fig = make_subplots(
        rows=1, cols=2, 
        column_widths=[0.7, 0.3],  
        subplot_titles=["Valor gasto por clientes", "Faturamento por clientes"],
        specs=[[{"type": "scatter"}, {"type": "table"}]] 
    )
    
    # Corrigir o nome da coluna 'Valor_individual_fmt_fmt'
    fig.add_trace(
        go.Bar(x=clientes1['nome'], y=clientes1['Valor_individual'], name='Valores', marker=dict(color=bar_colors)),
        row=1, col=1
    )
    
    # Corrigir o nome da coluna 'Valor_individual_fmt_fmt' na tabela
    fig.add_trace(
        go.Table(
            header=dict(values=['Valor_individual1', 'Nome do cliente']),
            cells=dict(values=[clientes1['Valor_individual1'], clientes1['nome']])
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        title_text="Clientes de maior valor",
        showlegend=False
    )
    clientes['data_pedido'] = pd.to_datetime(clientes['data_pedido'],dayfirst=True)
    clientes['mes'] = clientes['data_pedido'].dt.month
    clientes['ano'] = clientes['data_pedido'].dt.year
    teste_clientes = clientes[['nome', 'Valor_individual']].groupby(['nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False)
    teste_clientes['Valor_individual'] = teste_clientes['Valor_individual'].apply(format_currency)
    # Definir os dados (exemplo)
    client = clientes[['ano', 'mes', 'nome', 'Valor_individual']].reset_index(drop=True).sort_values(by='Valor_individual', ascending=False)
    clientes1 = client[(client['ano'] == 2024) & (client['mes'] == 1)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes2 = client[(client['ano'] == 2024) & (client['mes'] == 2)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes3 = client[(client['ano'] == 2024) & (client['mes'] == 3)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes4 = client[(client['ano'] == 2024) & (client['mes'] == 4)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes5 = client[(client['ano'] == 2024) & (client['mes'] == 5)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes6 = client[(client['ano'] == 2024) & (client['mes'] == 6)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes7 = client[(client['ano'] == 2024) & (client['mes'] == 7)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes8 = client[(client['ano'] == 2024) & (client['mes'] == 8)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes9 = client[(client['ano'] == 2024) & (client['mes'] == 9)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes10 = client[(client['ano'] == 2024) & (client['mes'] == 10)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes11= client[(client['ano'] == 2024) & (client['mes'] == 11)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    clientes12= client[(client['ano'] == 2024) & (client['mes'] == 12)].reset_index(drop=True).groupby(['ano', 'mes','nome'], as_index=False).sum().sort_values(by='Valor_individual', ascending=False).iloc[:10]
    melhores_clientes = pd.merge(clientes1[['nome']], clientes2[['nome']], on=list(clientes1[['nome']].columns), how='outer')
    melhores_clientes = pd.merge(melhores_clientes[['nome']], clientes3[['nome']], on='nome', how='outer')
    melhores_clientes = pd.merge(melhores_clientes[['nome']], clientes4[['nome']], on=list(clientes1[['nome']].columns), how='outer')
    melhores_clientes = pd.merge(melhores_clientes[['nome']], clientes5[['nome']], on=list(clientes1[['nome']].columns), how='outer')
    melhores_clientes = pd.merge(melhores_clientes[['nome']], clientes6[['nome']], on=list(clientes1[['nome']].columns), how='outer')
    melhores_clientes = pd.merge(melhores_clientes[['nome']], clientes7[['nome']], on=list(clientes1[['nome']].columns), how='outer')
    melhores_clientes = pd.merge(melhores_clientes[['nome']], clientes8[['nome']], on=list(clientes1[['nome']].columns), how='outer')
    melhores_clientes = pd.merge(melhores_clientes[['nome']], clientes9[['nome']], on=list(clientes1[['nome']].columns), how='outer')
    melhores_clientes = pd.merge(melhores_clientes[['nome']], clientes10[['nome']], on=list(clientes1[['nome']].columns), how='outer')
    melhores_clientes = pd.merge(melhores_clientes[['nome']], clientes11[['nome']], on=list(clientes1[['nome']].columns), how='outer')
    melhores_clientes = pd.merge(melhores_clientes[['nome']], clientes12[['nome']], on=list(clientes1[['nome']].columns), how='outer')
    
        # Dicionário com os DataFrames e nomes
    clientes_dict = {
        "Janeiro": clientes1,
        "Fevereiro": clientes2,
        "Março": clientes3,
        "Abril": clientes4,
        "Maio": clientes5,
        "Junho": clientes6,
        "Julho": clientes7,
        "Agosto": clientes8,
        "Setembro": clientes9,
        "Outubro": clientes10,
        "Novembro": clientes11,
        "Dezembro": clientes12,
    }
    
    # Formatar moeda
    def format_currency(value):
        return "${:,.2f}".format(value)
    
    if len(clientes1)+len(clientes2)+len(clientes3)+len(clientes4)+len(clientes5)+len(clientes6)+len(clientes7)+len(clientes8)+len(clientes9)+len(clientes10)>0:
    
        # Criar uma lista de gráficos válidos
        validos = [(mes, df) for mes, df in clientes_dict.items() if not df.empty]
        
        # Criar subgráficos com base no número de meses válidos
        sub = make_subplots(
            rows=len(validos),
            cols=2,
            column_widths=[1, 1.2],
            subplot_titles=sum([[f"Valor gasto por clientes - {mes}", f"Tabela - {mes}"] for mes, _ in validos], []),
            specs=[[{"type": "bar"}, {"type": "table"}] for _ in validos],
            row_heights=[0.08] * len(validos),
            vertical_spacing=0.02
        )
        
        # Definir cores por cliente
        all_nomes = pd.concat([df[['nome']] for _, df in validos])
        unique_descriptions = all_nomes['nome'].unique()
        colors = px.colors.qualitative.Set3
        if len(unique_descriptions) > len(colors):
            colors = px.colors.qualitative.Set2
        color_map = {desc: colors[i % len(colors)] for i, desc in enumerate(unique_descriptions)}
        
        # Preencher os subplots
        for i, (mes, df) in enumerate(validos):
            df['Valor_individual_fmt'] = df['Valor_individual'].apply(format_currency)
            bar_colors = [color_map[n] for n in df['nome']]
        
            sub.add_trace(
                go.Bar(x=df['nome'], y=df['Valor_individual'], name=f'Valores {mes}', marker=dict(color=bar_colors)),
                row=i+1, col=1
            )
            sub.add_trace(
                go.Table(
                    header=dict(values=['Valor_individual', 'Nome do cliente']),
                    cells=dict(values=[df['Valor_individual_fmt'], df['nome']])
                ),
                row=i+1, col=2
            )
        
        # Layout final
        sub.update_layout(
            title_text="Clientes de maior valor",
            showlegend=False,
            width=1200,
            height=300 * len(validos)
        )
        sub.update_xaxes(showticklabels=False)
    
    #prod = prod.head(10)
    prod1 = prod[['produto', 'Valor_individual']].groupby('produto',as_index=False).sum().sort_values(by='Valor_individual', ascending=False).head(10)
    colors = px.colors.qualitative.Set3
    unique_descriptions = prod1['produto'].unique()
    num_categories = len(unique_descriptions)
    if num_categories > len(colors):
        colors = px.colors.qualitative.Set2 
    color_map = {desc: colors[i % len(colors)] for i, desc in enumerate(unique_descriptions)}
    bar_colors = [color_map[desc] for desc in prod1['produto']]
    
    
    fig1 = make_subplots(
        rows=1, cols=2, 
        column_widths=[1000, 510],  
        subplot_titles=["Valor gasto por produto", "Faturamento/produto"],
        specs=[[{"type": "scatter"}, {"type": "table"}]] 
    )
    
    
    fig1.add_trace(
        go.Bar(x=prod1['produto'], y=prod1['Valor_individual'], name='Valores', marker=dict(color=bar_colors)),
        row=1, col=1
    )
    
    
    fig1.add_trace(
        go.Table(
            header=dict(values=['Valor_individual', 'Nome do produto']),
            cells=dict(values=[prod[['produto', 'Valor_individual']].groupby('produto',as_index=False).sum().sort_values(by='Valor_individual', ascending=False)['Valor_individual'].apply(format_currency), 
                               prod[['produto', 'Valor_individual']].groupby('produto',as_index=False).sum().sort_values(by='Valor_individual', ascending=False)['produto']])
        ),
        row=1, col=2
    )
    
    
    fig1.update_layout(
        title_text="produto por Faturamento",
        showlegend=False
    )
    
    
    
    # Exemplo simples de gráfico
    fig.write_html("static/grafico_clientes.html")
    sub.write_html("static/grafico_mensal.html")
    fig1.write_html("static/grafico_produtos.html")

    return templates.TemplateResponse("index.html", {"request": request, "show_plot": True})

