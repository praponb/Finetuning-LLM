from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "Chiang Mai"]) & (F.col("budget") >= 3857000))
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sum(F.when(F.col("location") == "London", 1).otherwise(0)).alias("london_count"),
        F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
    )
    .orderBy(F.col("avg_budget").desc(), F.col("london_count").desc())
)
