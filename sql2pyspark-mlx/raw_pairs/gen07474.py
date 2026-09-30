from pyspark.sql import functions as F

min_salary = df_employees.filter(F.col("salary").between(63000, 115000)).agg(F.min("salary")).first()[0]

result = (
    df_employees
    .filter(F.col("salary") == min_salary)
    .select("id", "salary")
)
