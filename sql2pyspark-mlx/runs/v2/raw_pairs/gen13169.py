from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3285000, "high").when(F.col("budget") >= 1656000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.count("*").alias("department_count"))
)
