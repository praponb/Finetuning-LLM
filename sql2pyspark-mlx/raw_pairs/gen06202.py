from pyspark.sql import functions as F

avg_salary = df_employees.agg(F.avg("salary")).first()[0]

result = (
    df_employees
    .filter(F.col("salary") >= avg_salary)
    .select("id", "salary", "hire_date")
    .orderBy(F.col("salary").desc(), F.col("id").desc())
)
