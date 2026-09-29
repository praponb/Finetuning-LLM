from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .pivot("job_title", ["Engineer", "Manager"])
    .agg(F.count("salary"))
    .orderBy("hire_month")
)
