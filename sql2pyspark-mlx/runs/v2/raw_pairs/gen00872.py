from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "CANCELLED")
    .select("id", "order_date", "channel", F.floor("discount").alias("discount_whole"))
)
