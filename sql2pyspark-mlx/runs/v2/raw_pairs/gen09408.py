from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .groupBy("dept_name")
    .agg(
        F.max("budget").alias("max_budget"),
        F.avg("budget").alias("avg_budget"),
    )
)
