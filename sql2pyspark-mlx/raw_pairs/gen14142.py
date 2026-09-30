from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNull() & (F.col("budget") < 2469000))
    .groupBy(
        F.when(F.col("budget") >= 4131000, "high").when(F.col("budget") >= 1344000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
    .filter(F.col("pct_london") >= 39)
    .orderBy(F.col("location").desc())
)
