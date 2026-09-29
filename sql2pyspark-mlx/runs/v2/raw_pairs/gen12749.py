from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") >= "2024-10-01") & F.month("opened_date").isin([10, 11, 12]))
    .select("city", F.trim("store_name").alias("clean_store_name"))
)
