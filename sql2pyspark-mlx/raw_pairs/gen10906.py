from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "New York") & F.col("location").isin(["Singapore", "New York"]))
    .select("id", "dept_name", "location")
)
