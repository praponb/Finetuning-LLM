from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3264000, "high").when(F.col("budget") >= 1272000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.count("budget").alias("budget_count"),
    )
    .filter(F.col("avg_budget") < 3318000)
    .orderBy(F.col("budget_count").desc(), "budget_bucket")
)
