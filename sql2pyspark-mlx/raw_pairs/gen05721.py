from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-08-19", "2025-01-01"))
    .select("salary")
)
