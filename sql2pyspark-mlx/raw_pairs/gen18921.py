from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3504000, "high").when(F.col("budget") >= 1395000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.sum(F.when(F.col("location") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
    )
    .filter(F.col("tokyo_count") < 65)
    .orderBy(F.col("budget_bucket").desc(), "location")
    .limit(10)
)
