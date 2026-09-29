from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "Singapore") & (F.col("budget") <= 3111000))
    .select("dept_name", "location")
)
