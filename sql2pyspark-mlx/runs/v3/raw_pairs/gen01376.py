from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("method")

result = (
    df_payments
    .groupBy("method")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("cumulative_amount").desc())
)
