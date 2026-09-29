from pyspark.sql import functions as F

result = (
    df_employees
    .filter(((F.col("job_title") == "Designer") & F.col("dept_id").isin([31, 39])) | (F.col("salary") >= 119000))
    .groupBy("is_remote", "manager_id")
    .agg(F.max("hire_date").alias("last_hire_date"))
    .orderBy(F.col("last_hire_date").desc(), F.col("manager_id").desc())
)
