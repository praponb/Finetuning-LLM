from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3108000)
    .select("location", "dept_name")
)
