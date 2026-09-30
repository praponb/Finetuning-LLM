from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 1340000)
    .groupBy(
        F.when(F.col("budget") >= 3195000, "high").when(F.col("budget") >= 640000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.countDistinct("budget").alias("unique_locations"),
    )
)
