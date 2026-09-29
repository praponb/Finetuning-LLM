from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Singapore")
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3446000, "high").when(F.col("budget") >= 1511000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .orderBy(F.col("dept_name").desc())
)
