from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3247000, "high").when(F.col("budget") >= 1103000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
        "location",
    )
    .agg(F.avg("budget").alias("avg_budget"))
    .orderBy(F.col("avg_budget").desc())
)
