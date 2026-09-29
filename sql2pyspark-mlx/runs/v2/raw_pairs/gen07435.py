from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3705000, "high").when(F.col("budget") >= 1865000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", F.col("budget")).otherwise(0)).alias("finance_budget"),
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
        F.min("budget").alias("min_budget"),
    )
    .filter(F.col("min_budget") > 2233000)
    .drop("min_budget")
    .orderBy(F.col("dept_name").desc())
)
