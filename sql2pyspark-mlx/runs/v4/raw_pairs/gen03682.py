from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status")

result = (
    df_payments
    .groupBy("status")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy("total_amount", "status")
    .limit(10)
)
