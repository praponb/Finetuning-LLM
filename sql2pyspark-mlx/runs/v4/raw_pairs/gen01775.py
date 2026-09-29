from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull() & F.col("job_title").isin(["Engineer", "Designer", "Intern"]))
    .select("dept_id", "salary", "email")
    .orderBy(F.col("dept_id").desc())
)
