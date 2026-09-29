from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") > "2024-03-24")
    .groupBy("channel")
    .agg(
        F.count("discount").alias("discount_count"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("amount")).otherwise(0)).alias("pending_amount"),
        F.max("order_date").alias("latest_order_date"),
    )
)
