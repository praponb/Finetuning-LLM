from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .orderBy("total_shipping_fee", "channel")
    .limit(10)
)
