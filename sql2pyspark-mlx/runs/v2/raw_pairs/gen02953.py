from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(F.max("salary").alias("max_salary"))
    .filter(F.col("max_salary") > 138000)
)
