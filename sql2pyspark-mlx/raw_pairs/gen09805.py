from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3429000, "high").when(F.col("budget") >= 1885000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.avg("budget").alias("avg_budget"))
    .filter(F.col("avg_budget") < 3421000)
    .select("budget_bucket", "avg_budget")
    .orderBy("avg_budget", "budget_bucket")
)
