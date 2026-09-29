from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status")

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.sum("discount").alias("total_discount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_discount", F.sum("total_discount").over(w))
    .orderBy(F.col("cumulative_discount").desc())
)
