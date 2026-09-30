from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("location")

result = (
    df_departments
    .filter(F.col("location") == "Singapore")
    .groupBy("location")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_budget", F.sum("total_budget").over(w))
)
