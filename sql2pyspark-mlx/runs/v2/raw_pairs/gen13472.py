from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city")
    .distinct()
    .crossJoin(df_stores.select("region").distinct())
)
