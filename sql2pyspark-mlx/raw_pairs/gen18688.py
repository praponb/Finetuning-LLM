from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_payments
    .filter(F.col("paid_at") >= "2024-07-01")
    .groupBy("method")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("rnk", F.dense_rank().over(w))
    .orderBy("rnk")
)
