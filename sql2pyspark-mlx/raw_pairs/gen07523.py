from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("dept_name") == "Finance")

summary = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 4100000, "high").when(F.col("budget") >= 676000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
)

result = summary
