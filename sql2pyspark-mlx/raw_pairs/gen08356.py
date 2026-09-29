from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["West", "North"]) & F.col("store_name").isNotNull())
    .select("opened_date", F.lower("store_name").alias("store_name_lower"))
)
