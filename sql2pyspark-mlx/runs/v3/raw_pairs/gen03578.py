from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 1369000)
    .groupBy(
        F.when(F.col("budget") >= 3953000, "high").when(F.col("budget") >= 1499000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
    )
    .filter(F.col("singapore_budget") >= 47)
    .select("budget_bucket", "singapore_budget")
)
