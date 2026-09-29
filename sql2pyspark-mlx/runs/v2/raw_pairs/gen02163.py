from pyspark.sql import functions as F

result = (
    df_departments
    .select("budget", "dept_name", F.lower("dept_name").alias("dept_name_lower"))
    .orderBy("dept_name_lower")
)
