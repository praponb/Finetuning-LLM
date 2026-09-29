from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3966000, "high").when(F.col("budget") >= 837000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
        "location",
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .filter(F.col("stddev_budget") > 77)
)
