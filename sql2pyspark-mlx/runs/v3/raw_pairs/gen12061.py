from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter((F.col("dept_name") == "Engineering") & F.col("location").isin(["New York", "London"]))
)

location_summary = (
    filtered_departments
    .groupBy("location")
    .agg(F.sum("budget").alias("total_budget"))
)

result = (
    location_summary
    .filter(F.col("total_budget") >= 14704000)
    .orderBy("location")
)
