from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.countDistinct("budget").alias("unique_locations"),
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget")).otherwise(0)).alias("bangkok_budget"),
    )
    .orderBy(F.col("bangkok_budget").desc(), F.col("unique_locations").desc())
)
