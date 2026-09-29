from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2024-07-01")
    .groupBy("job_title")
    .agg(F.avg("salary").alias("avg_salary"))
    .filter(F.col("avg_salary") >= 110000)
)
