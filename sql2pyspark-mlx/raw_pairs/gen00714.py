from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 39) & F.col("customer_id").isNull() & F.col("channel").isin(["store", "mobile"]))
    .groupBy(
        F.when(F.col("shipping_fee") >= 37, "high").when(F.col("shipping_fee") >= 10, "mid").otherwise("low").alias("shipping_fee_bucket"),
        "channel",
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") < 31)
)
