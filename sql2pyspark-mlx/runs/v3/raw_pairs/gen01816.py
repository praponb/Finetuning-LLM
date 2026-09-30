from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_month")

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .withColumn("cumulative_shipping_fee", F.sum("total_shipping_fee").over(w))
    .orderBy(F.col("order_month").desc(), F.col("total_shipping_fee").desc())
)
