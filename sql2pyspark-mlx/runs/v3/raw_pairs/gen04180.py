from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("total_budget") >= 16932000)
    .orderBy(F.col("max_budget").desc())
)
