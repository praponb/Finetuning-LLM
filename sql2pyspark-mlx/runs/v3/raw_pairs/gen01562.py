from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2651000, 2898000))
    .select("budget", "dept_name", "location")
    .orderBy("dept_name")
)
