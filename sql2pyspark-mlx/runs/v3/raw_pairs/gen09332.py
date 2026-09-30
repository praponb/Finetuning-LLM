from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("budget") <= 3549000)

location_summary = (
    recent_departments
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0)).alias("hr_count"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
    )
)

result = (
    location_summary
    .select("location", "hr_count", "berlin_count")
    .orderBy("location")
)
