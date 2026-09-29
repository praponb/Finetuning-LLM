from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") >= 0.28)
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(
        F.count("shipping_fee").alias("shipping_fee_count"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
    )
    .filter(F.col("shipping_fee_count") >= 2)
    .orderBy(F.col("shipping_fee_count").desc())
)
