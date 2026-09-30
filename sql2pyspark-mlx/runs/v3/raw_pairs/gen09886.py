from pyspark.sql import functions as F

filtered_orders = df_orders.filter(F.col("store_id").isNotNull())

summary = (
    filtered_orders
    .groupBy(
        F.when(F.col("discount") >= 0.31, "high").when(F.col("discount") >= 0.17, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
)

result = (
    summary
    .select("discount_bucket", "stddev_discount")
    .orderBy(F.col("stddev_discount").desc())
)
