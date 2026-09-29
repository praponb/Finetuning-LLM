from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.min("hire_date").alias("first_hire_date"),
        F.min("salary").alias("min_salary"),
    )
)
