from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_orders
    .filter(F.col("order_date") < "2025-07-07")
    .groupBy("store_id")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("amount_rank", F.dense_rank().over(w))
    .orderBy("store_id")
)
