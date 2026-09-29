from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", F.year("order_date").alias("order_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
)
