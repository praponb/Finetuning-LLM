from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .pivot("status", ["COMPLETED", "CANCELLED"])
    .agg(F.count("shipping_fee"))
    .orderBy("order_month")
)
