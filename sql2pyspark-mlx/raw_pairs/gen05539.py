from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(650, 1350))
    .groupBy(
        "status",
        F.when(F.col("shipping_fee") >= 33, "high").when(F.col("shipping_fee") >= 6, "mid").otherwise("low").alias("shipping_fee_bucket"),
        "channel",
    )
    .agg(
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
    .filter(F.col("completed_count") < 5)
)
