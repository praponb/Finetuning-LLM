from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3120000, "high").when(F.col("budget") >= 625000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("avg_budget") >= 2572000)
    .orderBy("budget_bucket", F.col("new_york_budget").desc())
)
