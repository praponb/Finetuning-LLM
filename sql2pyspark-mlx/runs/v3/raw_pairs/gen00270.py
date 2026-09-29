from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .groupBy("status", "store_id")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.count("shipping_fee").alias("shipping_fee_count"),
    )
    .orderBy(F.col("stddev_discount").desc(), "status", "store_id")
    .limit(10)
)
