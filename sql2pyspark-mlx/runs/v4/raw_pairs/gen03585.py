from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_shipping_fee").desc())

result = (
    df_orders
    .filter(F.col("shipping_fee") < 24)
    .groupBy(
        F.when(F.col("channel").isin(["store", "web"]), "core").otherwise("other").alias("channel_group"),
    )
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy("total_shipping_fee")
)
