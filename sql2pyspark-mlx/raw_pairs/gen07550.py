from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 44, "high").when(F.col("shipping_fee") >= 7, "mid").otherwise("low").alias("shipping_fee_bucket"),
        "channel",
    )
    .agg(
        F.sum("discount").alias("total_discount"),
        F.count("shipping_fee").alias("shipping_fee_count"),
    )
    .filter(F.col("shipping_fee_count") < 2)
    .drop("shipping_fee_count")
)
