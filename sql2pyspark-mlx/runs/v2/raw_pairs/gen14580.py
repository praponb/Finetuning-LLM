from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2258000, 2686000) & (F.col("location").isin(["Bangkok", "Chiang Mai", "Tokyo"]) | F.col("dept_name").isin(["Sales", "Legal"])))
    .groupBy(
        F.when(F.col("budget") >= 4122000, "high").when(F.col("budget") >= 2044000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
    )
)
