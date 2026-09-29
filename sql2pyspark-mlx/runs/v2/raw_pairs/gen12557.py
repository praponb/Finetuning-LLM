from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1719000)
    .select("id", "dept_name")
)
