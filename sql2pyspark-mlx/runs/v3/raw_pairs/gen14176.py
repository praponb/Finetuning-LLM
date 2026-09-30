from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "store_name", F.trim("store_name").alias("clean_store_name"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
