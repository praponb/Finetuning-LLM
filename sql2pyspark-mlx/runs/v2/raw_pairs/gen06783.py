from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
    )
    .filter(F.col("stddev_shipping_fee") < 26)
    .select("order_month", "stddev_shipping_fee", "pct_cancelled")
    .orderBy("order_month")
    .limit(5)
)
