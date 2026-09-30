from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("avg_budget") < 1780000)
    .select("dept_name", "avg_budget", "max_budget")
)
