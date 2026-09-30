from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"))
    .filter(F.col("avg_shipping_fee") >= 44)
    .orderBy("order_month")
    .limit(5)
)
