from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status")

result = (
    df_orders
    .filter(F.col("amount") >= 700)
    .groupBy("status")
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("cumulative_discount", F.sum("total_discount").over(w))
    .orderBy("status", F.col("total_discount").desc())
)
