from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("store_name").like("%Old%") | (F.col("city") != "Tokyo")))
    .orderBy("id", "store_name")
    .limit(1)
)
