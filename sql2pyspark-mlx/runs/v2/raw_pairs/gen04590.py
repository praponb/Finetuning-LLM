from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3508000, "high").when(F.col("budget") >= 724000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy(F.col("stddev_budget").desc(), F.col("budget_count").desc())
)
