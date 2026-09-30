from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3077000, "high").when(F.col("budget") >= 1266000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .orderBy(F.col("min_budget").desc())
)
