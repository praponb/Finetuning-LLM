from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1721000)
    .groupBy(
        F.when(F.col("budget") >= 3468000, "high").when(F.col("budget") >= 990000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Engineering", F.col("budget")).otherwise(0)).alias("engineering_budget"),
    )
    .orderBy(F.col("engineering_budget").desc())
)
