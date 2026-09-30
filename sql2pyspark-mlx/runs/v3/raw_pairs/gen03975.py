from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "New York") & (F.col("location") == "Bangkok") & (F.col("budget") >= 1655000))
    .select("id", "budget", "dept_name", F.trim("location").alias("clean_location"))
)
