from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") >= 0.35) & F.col("channel").isNotNull() & F.col("store_id").isin([1, 23]))
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("avg_shipping_fee") > 38)
    .orderBy(F.col("order_month").desc())
)
