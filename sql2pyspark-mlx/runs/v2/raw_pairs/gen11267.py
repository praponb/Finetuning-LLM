from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3933000, "high").when(F.col("budget") >= 1862000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("pct_chiang_mai") >= 79)
    .select("location", "budget_bucket", "pct_chiang_mai", "avg_budget")
    .orderBy(F.col("budget_bucket").desc())
)
