from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4302000, "high").when(F.col("budget") >= 1762000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum("budget").alias("total_budget"),
    )
    .orderBy(F.col("budget_bucket").desc())
)
