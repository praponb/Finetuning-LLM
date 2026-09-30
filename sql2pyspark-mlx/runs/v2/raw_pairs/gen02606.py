from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", "channel")
    .agg(
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
    )
    .orderBy(F.col("avg_shipping_fee").desc())
)
