from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)
