from pyspark.sql import functions as F

max_salary = df_employees.filter(F.col("hire_date") <= "2024-09-01").agg(F.max("salary")).first()[0]

result = (
    df_employees
    .filter(F.col("salary") > max_salary)
    .select("id", "salary", "email", "hire_date")
)
