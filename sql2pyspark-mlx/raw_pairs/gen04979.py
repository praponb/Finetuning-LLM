from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("store_id") == 12) & (F.col("channel") == "web"))
    .select("id", "discount", F.trim("status").alias("clean_status"))
)
