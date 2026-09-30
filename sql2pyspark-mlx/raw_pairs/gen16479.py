from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("dept_name").isin(["Legal", "Sales"]))

budget_bucket_summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 3359000, "high").when(F.col("budget") >= 2039000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
)

result = (
    budget_bucket_summary
    .filter(F.col("budget_range") >= 65)
    .orderBy("budget_range")
)
