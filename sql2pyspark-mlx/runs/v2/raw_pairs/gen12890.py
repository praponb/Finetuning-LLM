from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2845000, 3664000))
    .select("id", "dept_name", "location", F.trim("location").alias("clean_location"))
)
