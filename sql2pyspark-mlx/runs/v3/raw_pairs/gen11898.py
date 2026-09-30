from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3505000, "high").when(F.col("budget") >= 1264000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.avg("budget").alias("avg_budget"))
    .filter(F.col("avg_budget") > 1941000)
    .orderBy("avg_budget")
)
