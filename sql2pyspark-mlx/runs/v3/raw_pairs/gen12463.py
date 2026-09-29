from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 2738000) & (F.col("location") == "Berlin"))
    .groupBy("location")
    .agg(
        F.min("budget").alias("min_budget"),
        F.sum(F.when(F.col("location") == "Tokyo", F.col("budget")).otherwise(0)).alias("tokyo_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy(F.col("tokyo_budget").desc())
)
