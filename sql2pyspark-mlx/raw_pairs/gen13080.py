from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.min("budget").alias("min_budget"),
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
    )
    .orderBy(F.col("min_budget").desc())
)
