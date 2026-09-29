from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter((F.col("location") != "London") | (F.col("budget") >= 1739000))
)

summary = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 4364000, "high").when(F.col("budget") >= 1352000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.min("budget").alias("min_budget"),
    )
)

result = (
    summary
    .filter(F.col("budget_count") < 9)
    .orderBy(F.col("budget_bucket").desc())
    .limit(5)
)
