from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_shipping_fee").desc())

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1450, "high").when(F.col("amount") >= 590, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .withColumn("shipping_fee_rank", F.dense_rank().over(w))
    .orderBy(F.col("amount_bucket").desc())
)
