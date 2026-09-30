from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-08-30", "2024-06-01"))
    .select("id", "is_remote", "email")
    .orderBy(F.col("email").desc())
)
