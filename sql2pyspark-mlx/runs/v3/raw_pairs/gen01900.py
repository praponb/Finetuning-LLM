from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget_bucket")

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3938000, "high").when(F.col("budget") >= 1582000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.sum("budget").alias("total_budget"))
    .withColumn("cumulative_budget", F.sum("total_budget").over(w))
    .orderBy("cumulative_budget")
)
