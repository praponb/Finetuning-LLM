from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 3459000)
    .select("budget", "dept_name", "location")
    .orderBy("dept_name")
)
