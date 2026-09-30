from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-07-04", "2024-10-01"))
    .select("full_name", "salary")
)
