from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4021000, "high").when(F.col("budget") >= 916000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.sum("budget").alias("total_budget"),
    )
    .orderBy(F.col("total_budget").desc())
)
