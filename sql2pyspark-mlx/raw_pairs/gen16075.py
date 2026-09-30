from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.count("*").alias("order_count"),
        F.count("amount").alias("amount_count"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("shipping_fee")).otherwise(0)).alias("completed_shipping_fee"),
    )
    .orderBy("amount_count", F.col("order_count").desc(), "channel")
    .limit(10)
)
