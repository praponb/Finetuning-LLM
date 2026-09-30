from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3875000, "high").when(F.col("budget") >= 1884000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.avg("budget").alias("avg_budget"))
    .filter(F.col("avg_budget") < 3530000)
)
