from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@gmail.com") & F.col("salary").between(70000, 99000))
    .select("id", "manager_id", "job_title")
)
