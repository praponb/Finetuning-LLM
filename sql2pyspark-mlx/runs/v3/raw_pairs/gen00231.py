from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("city")

result = (
    df_customers
    .groupBy("city")
    .agg(F.sum("age").alias("total_age"))
    .withColumn("cumulative_age", F.sum("total_age").over(w))
    .orderBy(F.col("city").desc())
)
