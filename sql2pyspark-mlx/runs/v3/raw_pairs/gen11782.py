from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .agg(F.avg("shipping_fee").alias("avg_shipping_fee"))
    .filter(F.col("avg_shipping_fee") > 31)
    .select("order_month", "avg_shipping_fee")
    .orderBy("avg_shipping_fee", "order_month")
    .limit(5)
)
