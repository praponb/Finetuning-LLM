from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "web"]) & (F.col("channel") == "store"))
    .cube(
        "status",
        F.when(F.col("channel").isin(["web", "store"]), "tier_1").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.sum(F.when(F.col("channel") == "store", F.col("discount")).otherwise(0)).alias("store_discount"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
    .orderBy(F.col("store_discount").desc())
)
