from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 2087000)
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3371000, "high").when(F.col("budget") >= 1694000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
    .filter(F.col("stddev_budget") < 73)
    .select("dept_name", "budget_bucket", "stddev_budget")
)
