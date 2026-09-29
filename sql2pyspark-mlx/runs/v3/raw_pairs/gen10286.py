from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("dept_name") != "Sales")

departments_stats = (
    valid_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3201000, "high").when(F.col("budget") >= 1838000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
        F.count("*").alias("total_count"),
    )
)

result = (
    departments_stats
    .orderBy(F.col("singapore_budget").desc(), F.col("total_count").desc())
)
