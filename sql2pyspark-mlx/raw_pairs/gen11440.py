from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower("email").like("%enr%"))
    .select("id", "manager_id", "email", "hire_date")
)
