from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.max("budget").alias("max_budget"),
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget")).otherwise(0)).alias("chiang_mai_budget"),
        F.min("budget").alias("min_budget"),
    )
)
