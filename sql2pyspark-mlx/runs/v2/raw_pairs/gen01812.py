from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@mail.co.th") & F.col("job_title").isin(["Intern", "Manager"]))
    .select("id", "dept_id", "hire_date")
    .orderBy(F.col("dept_id").desc(), F.col("hire_date").desc(), "id")
    .limit(50)
)
