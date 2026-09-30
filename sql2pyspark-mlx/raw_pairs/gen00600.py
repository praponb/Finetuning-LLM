from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~(F.col("status").isin(["SHIPPED", "COMPLETED"]) & (F.col("discount") > 0.11)))
    .rollup(
        "channel",
        F.when(F.col("status").isin(["COMPLETED", "RETURNED"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(
        F.min("shipping_fee").alias("min_shipping_fee"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("discount")).otherwise(0)).alias("returned_discount"),
    )
    .orderBy("status_group")
)
