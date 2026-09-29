from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Finance") & F.col("location").isNotNull())
    .groupBy("location")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("stddev_budget") < 74)
)
