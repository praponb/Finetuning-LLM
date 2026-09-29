from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3530000, "high").when(F.col("budget") >= 1381000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
    .orderBy(F.col("min_budget").desc(), "dept_name", "budget_bucket")
    .limit(20)
)
