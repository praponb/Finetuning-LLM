from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .groupBy(
        F.when(F.col("budget") >= 4169000, "high").when(F.col("budget") >= 2017000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.countDistinct("budget").alias("unique_locations"),
    )
)
