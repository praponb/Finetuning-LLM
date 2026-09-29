from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") < "2023-05-01") & (F.col("job_title") == "Designer"))
    .groupBy("manager_id")
    .agg(F.max("hire_date").alias("last_hire_date"))
)
