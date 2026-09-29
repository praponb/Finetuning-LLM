from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("budget").isNotNull())

departments_stats = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 3847000, "high").when(F.col("budget") >= 1360000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)

result = (
    departments_stats
    .filter(F.col("avg_budget") >= 40)
    .select("budget_bucket", "total_budget", "avg_budget")
    .orderBy("budget_bucket", F.col("avg_budget").desc())
)
