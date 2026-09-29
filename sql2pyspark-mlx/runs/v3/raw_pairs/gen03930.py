from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3981000, "high").when(F.col("budget") >= 650000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
    )
    .orderBy(F.col("budget_bucket").desc())
)
