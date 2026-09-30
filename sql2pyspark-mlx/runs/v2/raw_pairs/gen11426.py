from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
    .filter(F.col("stddev_discount") >= 46)
    .select("order_month", "stddev_discount")
    .orderBy("order_month")
)
