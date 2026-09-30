from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.avg("shipping_fee").alias("avg_shipping_fee"),
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
    .orderBy(F.col("avg_shipping_fee").desc(), F.col("partner_count").desc(), "channel")
    .limit(10)
)
