from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2976000, 3853000))
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3640000, "high").when(F.col("budget") >= 1133000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.min("budget").alias("min_budget"),
    )
    .orderBy("dept_name")
)
