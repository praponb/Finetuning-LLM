from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("location")

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .groupBy("location")
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("cumulative_budget", F.sum("total_budget").over(w))
    .orderBy(F.col("cumulative_budget").desc(), F.col("location").desc())
)
