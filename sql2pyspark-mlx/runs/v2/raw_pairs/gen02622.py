from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3759000, "high").when(F.col("budget") >= 2045000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.countDistinct("budget").alias("unique_locations"),
        F.avg("budget").alias("avg_budget"),
    )
    .orderBy(F.col("unique_locations").desc())
)
