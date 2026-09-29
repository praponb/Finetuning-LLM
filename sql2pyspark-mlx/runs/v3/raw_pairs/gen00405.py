from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget")).otherwise(0)).alias("chiang_mai_budget"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("chiang_mai_budget").desc())
)
