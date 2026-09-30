from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("budget").between(1822000, 2229000))

summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 4398000, "high").when(F.col("budget") >= 1249000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count("budget").alias("budget_count"),
    )
)

result = (
    summary
    .filter(F.col("avg_budget") >= 40)
    .select("budget_bucket", "avg_budget", "budget_count")
    .orderBy("budget_count")
)
