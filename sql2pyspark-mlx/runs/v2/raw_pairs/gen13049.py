from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Legal")
    .groupBy(
        F.when(F.col("budget") >= 3678000, "high").when(F.col("budget") >= 1529000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count("*").alias("department_count"),
        F.min("budget").alias("min_budget"),
    )
)
