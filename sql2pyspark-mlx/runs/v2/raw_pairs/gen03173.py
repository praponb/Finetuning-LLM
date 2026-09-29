from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.max("budget").alias("max_budget"),
        F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0)).alias("hr_count"),
    )
)
