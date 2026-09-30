from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4503000, "high").when(F.col("budget") >= 868000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .orderBy(F.col("avg_budget").desc())
)
