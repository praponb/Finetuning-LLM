from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3581000, "high").when(F.col("budget") >= 1723000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count(F.when(F.col("location") == "London", 1)).alias("london_count"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
    )
    .filter(F.col("hr_count") < 1)
    .drop("hr_count")
)
