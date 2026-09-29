from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "London") & (F.col("location") == "New York"))
    .select("id", "budget", "dept_name", "location")
)
