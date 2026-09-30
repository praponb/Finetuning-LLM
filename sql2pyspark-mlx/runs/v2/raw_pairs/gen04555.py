from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget")).otherwise(0)).alias("chiang_mai_budget"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .filter(F.col("chiang_mai_budget") > 54)
    .orderBy(F.col("location").desc())
)
