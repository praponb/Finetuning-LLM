from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@mail.co.th"))
    .select("job_title")
    .subtract(df_departments.select(F.col("dept_name").alias("job_title")))
    .orderBy(F.col("job_title").desc())
)
