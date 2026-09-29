from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([1, 4, 5]))
    .groupBy(
        "status",
        "channel",
        F.when(F.col("amount") >= 1800, "high").when(F.col("amount") >= 690, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.max("order_date").alias("last_order_date"),
    )
    .orderBy("amount_bucket", "status")
)
