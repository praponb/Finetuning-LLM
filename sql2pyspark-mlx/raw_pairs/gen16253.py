from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_id")

result = (
    df_payments
    .filter(F.col("paid_at") >= "2024-04-10")
    .groupBy("order_id")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
)
