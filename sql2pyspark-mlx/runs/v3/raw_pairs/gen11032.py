from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull() & F.col("status").isin(["SHIPPED", "PENDING", "COMPLETED"]))
    .groupBy(
        "channel",
        F.when(F.col("discount") >= 0.31, "high").when(F.col("discount") >= 0.06, "mid").otherwise("low").alias("discount_bucket"),
        "status",
    )
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
    .filter(F.col("stddev_discount") > 56)
    .orderBy("stddev_discount")
)
