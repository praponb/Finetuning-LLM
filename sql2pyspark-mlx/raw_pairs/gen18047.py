from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.year("order_date") >= 2025) & (F.col("amount") < 410))
    .select("channel", "status", F.length("status").alias("status_length"))
    .orderBy("channel")
)
