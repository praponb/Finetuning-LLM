from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull() & (F.col("dept_name") == "Engineering"))
    .select("id", "location", "dept_name", F.upper("location").alias("location_upper"))
    .orderBy("location")
)
