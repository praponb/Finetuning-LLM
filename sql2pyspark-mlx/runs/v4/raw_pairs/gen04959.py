from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Legal") | (F.col("location") == "Singapore"))
    .select("id", "dept_name")
)
