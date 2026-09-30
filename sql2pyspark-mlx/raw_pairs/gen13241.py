from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "New York") & (F.col("budget") > 3643000) & (F.col("budget") >= 2170000))
    .select("budget", F.lower("dept_name").alias("dept_name_lower"))
)
