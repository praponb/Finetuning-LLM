from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.sum(F.when(F.col("location") == "New York", 1).otherwise(0)).alias("new_york_count"),
    )
    .orderBy(F.col("avg_budget").desc())
)
