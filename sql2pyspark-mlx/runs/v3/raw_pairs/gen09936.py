from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 2081000) & F.col("dept_name").isin(["Engineering", "Legal"]))
    .groupBy(
        F.when(F.col("budget") >= 4055000, "high").when(F.col("budget") >= 1778000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
)
