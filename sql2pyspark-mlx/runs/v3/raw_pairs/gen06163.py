from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Do%") & F.col("region").isNotNull())
    .select("id", "store_name", "sqft", "region")
    .orderBy(F.col("store_name").desc(), "id")
    .limit(10)
)
