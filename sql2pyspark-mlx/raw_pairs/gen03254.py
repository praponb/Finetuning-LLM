from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.max("budget").alias("max_budget"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("avg_budget") > 2948000)
    .orderBy(F.col("avg_budget").desc(), "max_budget")
)
