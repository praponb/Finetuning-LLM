from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_discount").desc())

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("discount_rank", F.rank().over(w))
    .orderBy("discount_rank")
)
