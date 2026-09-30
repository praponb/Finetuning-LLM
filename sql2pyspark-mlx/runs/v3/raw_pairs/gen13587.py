from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4052000, "high").when(F.col("budget") >= 1177000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg((F.max("budget") - F.min("budget")).alias("budget_range"))
    .filter(F.col("budget_range") >= 56)
    .select("budget_bucket", "dept_name", "budget_range")
)
