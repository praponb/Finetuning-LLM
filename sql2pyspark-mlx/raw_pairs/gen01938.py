from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4208000, "high").when(F.col("budget") >= 1369000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineering"),
    )
    .filter(F.col("pct_engineering") >= 68)
)
