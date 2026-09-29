from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") <= "2025-09-01")
    .groupBy("channel")
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") > 900)
    .select("channel", "avg_amount")
)
