from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4396000, "high").when(F.col("budget") >= 1902000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"))
    .filter(F.col("tokyo_count") > 6)
    .orderBy(F.col("budget_bucket").desc())
)
