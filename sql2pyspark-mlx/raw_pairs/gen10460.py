from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4327000, "high").when(F.col("budget") >= 1729000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.count("budget").alias("budget_count"))
    .filter(F.col("budget_count") > 5)
    .orderBy(F.col("budget_count").desc(), "budget_bucket")
    .limit(10)
)
