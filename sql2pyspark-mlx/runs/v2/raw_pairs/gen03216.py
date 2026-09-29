from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("budget") < 2573000) & ((F.col("budget") <= 1672000) | (F.col("location") == "New York")))
)

departments_stats = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 3116000, "high").when(F.col("budget") >= 2011000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
)

result = (
    departments_stats
    .select("budget_bucket", "budget_count")
    .orderBy(F.col("budget_bucket").desc())
)
