from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.max("budget").alias("max_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .filter(F.col("avg_budget") > 37)
    .select("dept_name", "max_budget", "avg_budget")
    .orderBy("dept_name")
)
