from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Manager")
    .groupBy("is_remote")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)
