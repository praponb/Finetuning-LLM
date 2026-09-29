from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Tokyo"]) & F.col("dept_name").isin(["Finance", "Sales"]) & (F.col("budget") >= 3434000))
    .groupBy(
        F.when(F.col("budget") >= 3374000, "high").when(F.col("budget") >= 1657000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
)
