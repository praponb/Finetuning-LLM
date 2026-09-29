from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .orderBy(F.col("avg_budget").desc())
)
