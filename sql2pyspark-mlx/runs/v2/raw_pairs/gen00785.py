from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .pivot("job_title", ["Analyst", "Designer", "Manager"])
    .agg(F.count("salary"))
)
