from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget")).otherwise(0)).alias("chiang_mai_budget"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .filter(F.col("unique_locations") > 4)
    .drop("unique_locations")
)
