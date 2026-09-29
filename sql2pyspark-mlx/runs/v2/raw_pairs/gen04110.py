from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter((F.col("location") != "Singapore") & (F.col("location") == "Tokyo"))
)

location_summary = (
    recent_departments
    .groupBy("location")
    .agg(F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"))
)

result = location_summary
