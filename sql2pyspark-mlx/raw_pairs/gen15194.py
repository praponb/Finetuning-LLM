from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNull())
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"))
)
