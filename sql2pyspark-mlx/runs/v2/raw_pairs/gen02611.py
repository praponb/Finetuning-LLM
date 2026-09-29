from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("location").isNull() & (F.col("dept_name") != "Sales"))

budget_bucket_summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 3178000, "high").when(F.col("budget") >= 1737000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
)

result = budget_bucket_summary
