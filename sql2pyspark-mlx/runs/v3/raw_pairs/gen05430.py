from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Finance", "Marketing"]))
    .groupBy("location")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)
