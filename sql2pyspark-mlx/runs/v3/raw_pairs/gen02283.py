from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("budget").between(1571000, 1619000) & (F.col("budget") >= 2246000))
)

location_summary = (
    valid_departments
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.min("budget").alias("min_budget"),
    )
)

result = (
    location_summary
    .filter(F.col("tokyo_count") > 5)
    .orderBy("min_budget", F.col("tokyo_count").desc(), "location")
    .limit(10)
)
