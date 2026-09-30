from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .groupBy("status")
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
    .filter(F.col("stddev_discount") > 46)
    .select("status", "shipping_fee_count", "stddev_discount")
    .orderBy(F.col("stddev_discount").desc())
)
