from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "status",
        "channel",
        F.when(F.col("discount") >= 0.42, "high").when(F.col("discount") >= 0.17, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.count("shipping_fee").alias("shipping_fee_count"))
    .orderBy(F.col("discount_bucket").desc())
)
