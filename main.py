import apache_beam as beam
import re

from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText, WriteToCsv
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_dengue = ['id', 'data_iniSE', 'casos', 'ibge_code', 'cidade',
                  'uf', 'latitude', 'longitude']

def list_to_dict(elemento, colunas):
    """
    Recebe 2 listas
    Retorna um dict
    """
    return dict(zip(colunas, elemento))

def tex_to_list(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    
    return elemento.split(delimitador)

def trata_data(elemento):
    """
    Recebe um dict e cria um novo campo com ANO-MES
    Retorna o dict com o novo campo
    """
    elemento["ano_mes"] = "-".join(elemento["data_iniSE"].split("-")[:2])
    return elemento

def chave_uf(elemento):
    """
    Receber um dict
    Retornar uma tupla com estado(UF) e o elemento (UF, dict)
    """
    chave = elemento["uf"]
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS', [{}, {}])
    Retorna uma tupla ('RS-2014-12', 8.0)
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            yield (f"{uf}-{registro['ano_mes']}", 0.0)
            
def chave_uf_ano_mes_lista(elemento):
    """
    Recebe uma lista de elementos
    Retorna uma tupla conetndo uma chave e o valor da chuva em mm
    ('UF-ANO-MES', 1.3)
    """
    data, mm, uf = elemento
    ano_mes = "-".join(data.split("-")[:2])
    chave = f"{uf}-{ano_mes}"
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
        
    return chave, float(mm)

def arredondar(elemento):
    """
    Recebe uma tupla
    Retornar uma tupla com valor arredondado
    """
    chave, mm = elemento
    return (chave, round(mm, 1))

def filtrar_campos_vazios(elemento):
    """
    Remove elementos que tenham chave vazio
    Retorna uma tupla
    """
    
    chave, dados = elemento
    # Caso tenha dados no campo chuvas e dengue retornar True
    if all([
        dados['chuvas'],
        dados['dengue']
        ]):
        return True
    
    return False

def descompactar_elementos(elem):
    """
    Recebe um tupla compactada 
    Retorna uma tupla com os dados descompactados
    """
    chave, dados = elem
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    
    return uf, ano, mes, str(chuva), str(dengue)

def preparar_csv(elem, delimitador=';'):
    """
    Receber uma tupla
    Retornar uma string delimitada
    """
    
    return f"{delimitador}".join(elem)

dengue = (
    pipeline
    | "Leitura do dataset de dengue" >> 
        ReadFromText('casos_dengue2.txt', skip_header_lines=1)
    | "Transformação em lista" >> beam.Map(tex_to_list, delimitador='|')
    |  "Transformação em dicionário" >> beam.Map(list_to_dict, colunas_dengue)
    | "Criar campo ano_mes" >> beam.Map(trata_data)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    # |  "Print" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do dataset de chuva" >> 
        ReadFromText('chuvas2.csv', skip_header_lines=1)
    | "Transformação em lista (chuvas)" >> beam.Map(tex_to_list, delimitador=',')
    | "Criando chave UF-ANO-MES" >> beam.Map(chave_uf_ano_mes_lista)
    | "Soma dos total de chuvas pela chave" >> beam.CombinePerKey(sum)
    | "Arredondar resultados de chuvas" >> beam.Map(arredondar)
    # |  "Print chuvas" >> beam.Map(print)
    
)

resultado = (
    #(chuvas, dengue)
    #| "Empilha as pcols" >> beam.Flatten() # Flatten empilha 2 pcols
    #| "Agrupa as pcols" >> beam.GroupByKey()
    ({'chuvas': chuvas, 'dengue': dengue})
    | "Mesclar pcols" >> beam.CoGroupByKey() # CoGroupByKey empilha e agrupa pela chave
    | "Filtrar dados vazios" >> beam.Filter(filtrar_campos_vazios) # Filter -> Espera um retorno bool pra saber se deixa o dado ou não
    | "Descompactar elementos" >> beam.Map(descompactar_elementos)
    | "Preparar CSV" >> beam.Map(preparar_csv)
    
    # | "Print" >> beam.Map(print)
)

header = "UF;ANO;MES;CHUVA;DENGUE"
resultado | "Criar arquivo CSV" >> WriteToText("reusltado", 
                                    file_name_suffix='.csv', header=header)

pipeline.run()