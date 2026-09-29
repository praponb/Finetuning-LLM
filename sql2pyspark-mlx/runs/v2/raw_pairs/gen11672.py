from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2025-01-01")
    .select("id", "is_remote", "manager_id")
    .orderBy(F.col("manager_id").desc(), "id")
)
