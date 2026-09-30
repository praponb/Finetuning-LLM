from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1793000)
    .groupBy(
        F.when(F.col("budget") >= 4265000, "high").when(F.col("budget") >= 1239000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"))
    .filter(F.col("tokyo_count") >= 2)
    .orderBy("tokyo_count", "budget_bucket")
    .limit(5)
)
