from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile"]) | (F.col("discount") < 0.14))
)

orders_stats = (
    src
    .groupBy("status")
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.sum(F.when(F.col("channel") == "store", F.col("discount")).otherwise(0)).alias("store_discount"),
    )
)

result = (
    orders_stats
    .select("status", "avg_shipping_fee")
    .orderBy(F.col("avg_shipping_fee").desc())
)
