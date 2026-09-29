from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .select("id", "budget", "location", "dept_name")
)
