from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_shipping_fee").desc())

result = (
    df_orders
    .filter(F.col("discount").between(0.1, 0.18))
    .groupBy("channel")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .withColumn("shipping_fee_rank", F.dense_rank().over(w))
)
