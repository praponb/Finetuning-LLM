from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.date_format("order_date", "yyyy-MM").alias("order_month"))
    .pivot("channel", ["mobile", "store", "partner", "web"])
    .agg(F.max("amount"))
    .orderBy("order_month")
)
