from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id", "status", "channel")
    .agg(
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        F.countDistinct("amount").alias("unique_channels"),
    )
    .filter(F.col("stddev_discount") >= 27)
    .orderBy("unique_channels", "store_id", "status", "channel")
    .limit(20)
)
