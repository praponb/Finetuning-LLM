from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status")

result = (
    df_orders
    .groupBy("status")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .withColumn("cumulative_shipping_fee", F.sum("total_shipping_fee").over(w))
    .orderBy("total_shipping_fee", F.col("status").desc())
)
