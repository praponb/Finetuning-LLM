from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget").isNotNull() & (F.col("budget") < 2939000)) | (F.col("budget") >= 2267000))
    .select("id", "dept_name", "budget", "location")
)
