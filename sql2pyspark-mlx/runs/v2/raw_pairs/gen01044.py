from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Designer")
    .select("id", "manager_id", F.floor("salary").alias("salary_whole"))
    .orderBy(F.col("manager_id").desc())
)
