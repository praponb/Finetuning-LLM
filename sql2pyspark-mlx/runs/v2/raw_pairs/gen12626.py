from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("avg_budget") > 2343000)
    .select("location", "total_budget", "avg_budget")
)
