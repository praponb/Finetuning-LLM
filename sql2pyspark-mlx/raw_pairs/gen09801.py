from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2023-05-01")
    .select("dept_id", "manager_id", "level")
    .orderBy(F.col("manager_id").desc())
)
