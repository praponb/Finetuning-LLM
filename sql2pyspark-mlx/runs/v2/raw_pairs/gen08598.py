from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-04-20", "2025-04-01"))
    .select("full_name")
    .intersect(df_employees.filter(F.col("manager_id") == 12).select("full_name"))
)
