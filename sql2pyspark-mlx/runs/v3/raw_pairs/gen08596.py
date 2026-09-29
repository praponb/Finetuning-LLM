from pyspark.sql import functions as F

base = df_departments.filter(F.col("budget") >= 3748000)

budget_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("budget") >= 4051000, "high").when(F.col("budget") >= 1278000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
    )
)

result = (
    budget_bucket_summary
    .filter(F.col("total_budget") < 11866000)
    .orderBy(F.col("singapore_budget").desc())
)
