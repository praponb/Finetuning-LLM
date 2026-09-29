from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget_bucket")

result = (
    df_departments
    .filter(F.col("budget") > 3744000)
    .groupBy(
        F.when(F.col("budget") >= 3081000, "high").when(F.col("budget") >= 1546000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_budget", F.sum("total_budget").over(w))
    .orderBy(F.col("budget_bucket").desc(), F.col("cumulative_budget").desc())
)
