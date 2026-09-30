from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNull() & F.col("dept_name").isin(["Marketing", "Legal", "Sales"]))
    .groupBy(
        F.when(F.col("budget") >= 3814000, "high").when(F.col("budget") >= 1377000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(F.round(F.avg("budget"), 2).alias("avg_budget"))
    .orderBy("budget_bucket")
)
