from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 3794000)
    .groupBy(
        F.when(F.col("budget") >= 4151000, "high").when(F.col("budget") >= 1557000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
)
