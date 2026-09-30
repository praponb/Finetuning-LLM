from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("dept_name") != "Finance") & (F.col("dept_name") != "Legal"))
)

departments_stats = (
    valid_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3395000, "high").when(F.col("budget") >= 818000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)

result = (
    departments_stats
    .filter(F.col("budget_range") > 45)
    .select("location", "budget_bucket", "singapore_count", "budget_range")
)
