from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .groupBy("job_title", F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
    .orderBy("hire_month")
)
