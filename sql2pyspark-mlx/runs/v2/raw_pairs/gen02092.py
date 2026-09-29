from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "budget", "dept_name")
    .orderBy("budget", F.col("dept_name").desc())
)
