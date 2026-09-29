from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .filter(F.col("budget_range") > 61)
    .orderBy(F.col("stddev_budget").desc())
)
