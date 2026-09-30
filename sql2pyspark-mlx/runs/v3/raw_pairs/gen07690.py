from pyspark.sql import functions as F

avg_salary = df_employees.agg(F.avg("salary")).first()[0]

result = (
    df_employees
    .filter(F.col("salary") >= avg_salary)
    .select("id", "salary", "job_title", "email")
    .orderBy(F.col("job_title").desc())
)
