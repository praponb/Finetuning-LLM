from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull() & (F.col("dept_id") == 16) & (F.col("job_title") != "Engineer"))
    .select("id", "manager_id")
    .orderBy(F.col("id").desc(), F.col("manager_id").desc())
)
