from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .distinct()
    .crossJoin(df_stores.select("region").distinct())
    .orderBy("city", "region")
)
