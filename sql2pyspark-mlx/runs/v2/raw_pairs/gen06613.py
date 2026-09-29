from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .groupBy(
        F.when(F.col("budget") >= 3077000, "high").when(F.col("budget") >= 1541000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0)).alias("hr_count"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("max_budget") > 1384000)
    .orderBy(F.col("hr_count").desc(), F.col("max_budget").desc())
)
