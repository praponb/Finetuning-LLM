from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 2644000) & (F.col("dept_name") == "Engineering") & (F.col("location") == "London"))
    .select("id", "dept_name", "location")
)
