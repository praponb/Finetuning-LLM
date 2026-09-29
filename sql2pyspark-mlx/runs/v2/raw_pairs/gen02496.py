from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_shipping_fee").desc())

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy("status")
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.count("*").alias("cnt"),
    )
    .withColumn("shipping_fee_rank", F.rank().over(w))
    .orderBy(F.col("shipping_fee_rank").desc())
)
