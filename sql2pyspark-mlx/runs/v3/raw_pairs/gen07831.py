from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Tokyo", "London"]))
    .select("id", "budget", "dept_name", F.trim("location").alias("clean_location"))
)
