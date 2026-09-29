from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") != "Chiang Mai")
    .groupBy(
        F.when(F.col("budget") >= 3841000, "high").when(F.col("budget") >= 1803000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_locations"),
        F.count("budget").alias("budget_count"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy("budget_bucket")
)
