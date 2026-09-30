from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Berlin", "New York"]) & (F.col("budget") <= 1285000))
)

summary = (
    filtered_departments
    .groupBy("location")
    .agg(
        F.countDistinct("budget").alias("unique_locations"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
)

result = (
    summary
    .select("location", "unique_locations", "stddev_budget")
    .orderBy("unique_locations")
)
