from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
        F.sum("shipping_fee").alias("total_shipping_fee"),
    )
    .filter(F.col("total_shipping_fee") >= 140)
    .orderBy("avg_amount")
)
