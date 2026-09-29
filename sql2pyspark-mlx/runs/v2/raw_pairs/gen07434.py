from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("channel")

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_shipping_fee", F.sum("total_shipping_fee").over(w))
    .orderBy("channel", F.col("total_shipping_fee").desc())
)
