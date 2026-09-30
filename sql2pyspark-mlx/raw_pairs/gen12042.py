from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 3193000) | (F.col("budget") <= 1775000))
    .groupBy(
        F.when(F.col("budget") >= 4215000, "high").when(F.col("budget") >= 899000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.avg("budget").alias("avg_budget"),
    )
    .orderBy("avg_budget")
)
