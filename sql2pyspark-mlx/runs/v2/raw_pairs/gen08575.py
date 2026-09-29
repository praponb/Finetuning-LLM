from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2860000)
    .groupBy(
        F.when(F.col("budget") >= 4301000, "high").when(F.col("budget") >= 1257000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.countDistinct("budget").alias("unique_locations"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("avg_budget") < 2605000)
    .drop("avg_budget")
)
