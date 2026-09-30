from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("location") == "New York")

location_summary = (
    recent_departments
    .groupBy("location")
    .agg(F.avg("budget").alias("avg_budget"))
)

result = location_summary.filter(F.col("avg_budget") >= 3412000)
