from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_discount").desc())

result = (
    df_orders
    .groupBy("status")
    .agg(F.sum("discount").alias("total_discount"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("total_discount").desc())
)
