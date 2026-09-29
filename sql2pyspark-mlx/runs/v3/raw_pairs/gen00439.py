from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("channel")

result = (
    df_orders
    .filter(F.col("amount") <= 1040)
    .groupBy("channel")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
)
