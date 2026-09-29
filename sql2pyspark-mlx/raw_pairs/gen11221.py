from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") <= 126000)
    .groupBy("job_title")
    .agg(F.max("hire_date").alias("latest_hire_date"))
    .orderBy("latest_hire_date")
)
