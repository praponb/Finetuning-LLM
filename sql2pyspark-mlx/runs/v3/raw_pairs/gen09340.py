from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 2415000) & F.col("budget").between(1861000, 3320000))
    .groupBy("dept_name")
    .agg(
        F.max("budget").alias("max_budget"),
        F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0)).alias("hr_count"),
    )
    .orderBy("max_budget", "dept_name")
    .limit(10)
)
