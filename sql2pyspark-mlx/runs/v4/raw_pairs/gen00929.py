from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .select("budget", "location", "dept_name")
)
