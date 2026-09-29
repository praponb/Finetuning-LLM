from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull() | (F.col("budget") > 2070000))
    .groupBy(
        F.when(F.col("budget") >= 4116000, "high").when(F.col("budget") >= 1282000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .orderBy(F.col("budget_bucket").desc())
)
