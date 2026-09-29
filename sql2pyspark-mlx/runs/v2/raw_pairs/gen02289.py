from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-08-01", "2024-12-01") & (F.col("salary") > 98000))
    .select("id", "dept_id")
)
