from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .select("id", "store_id", "discount", F.lower("status").alias("status_lower"))
)
