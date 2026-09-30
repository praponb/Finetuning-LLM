from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "Singapore")
    .groupBy(
        F.when(F.col("budget") >= 3682000, "high").when(F.col("budget") >= 983000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
    )
    .orderBy(F.col("singapore_budget").desc())
)
