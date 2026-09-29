from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "Tokyo")
    .groupBy(
        F.when(F.col("budget") >= 3416000, "high").when(F.col("budget") >= 1439000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(F.count("budget").alias("budget_count"))
    .orderBy(F.col("budget_count").desc(), F.col("budget_bucket").desc())
)
