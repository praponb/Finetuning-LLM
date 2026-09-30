from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "HR")
    .groupBy(
        F.when(F.col("budget") >= 4469000, "high").when(F.col("budget") >= 1111000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.max("budget").alias("max_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)
