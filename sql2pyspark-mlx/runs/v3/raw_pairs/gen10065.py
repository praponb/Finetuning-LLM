from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Singapore")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.max("budget").alias("max_budget"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .orderBy(F.col("budget_range").desc())
)
