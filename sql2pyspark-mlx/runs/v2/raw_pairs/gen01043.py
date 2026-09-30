from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("country")
    .distinct()
    .crossJoin(df_stores.select("region").distinct())
    .orderBy("country", "region")
)
