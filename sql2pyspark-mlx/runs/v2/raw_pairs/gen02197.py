from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") >= "2025-08-01")
    .select("id", "salary", "dept_id")
)
