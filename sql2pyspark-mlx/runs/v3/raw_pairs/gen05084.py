from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 1342000) & (F.col("budget") < 3747000))
    .select("location", "dept_name")
)
