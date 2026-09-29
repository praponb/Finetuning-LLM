from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2024-12-01", "2024-12-05") & F.col("is_remote"))
    .select("level", "is_remote", "full_name")
)
