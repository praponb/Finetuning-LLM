from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("Jac%") | (F.col("job_title") == "Director"))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
)
