from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .groupBy(
        F.when(F.col("budget") >= 3185000, "high").when(F.col("budget") >= 1560000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("department_count"),
        F.min("budget").alias("min_budget"),
    )
)
