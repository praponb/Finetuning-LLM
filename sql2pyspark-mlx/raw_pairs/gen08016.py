from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.max("budget").alias("max_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)
