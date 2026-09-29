from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNull())
    .cube("status", F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)
