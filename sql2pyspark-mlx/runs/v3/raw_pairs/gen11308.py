from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_shipping_fee").desc())

result = (
    df_orders
    .groupBy(F.month("order_date").alias("order_month"))
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
    .withColumn("shipping_fee_rank", F.rank().over(w))
)
