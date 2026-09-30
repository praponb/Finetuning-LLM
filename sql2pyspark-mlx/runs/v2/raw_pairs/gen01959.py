from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_shipping_fee").desc())

result = (
    df_orders
    .filter(F.col("order_date") <= "2025-10-28")
    .groupBy("channel")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("total_shipping_fee").desc(), "channel")
    .limit(5)
)
