from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("location") != "New York") & (F.col("budget") > 3857000)) | (F.col("dept_name") == "HR"))
    .select("budget", "dept_name", "location")
    .orderBy("location")
)
