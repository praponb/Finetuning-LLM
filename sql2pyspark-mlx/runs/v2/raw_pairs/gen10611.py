from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 12)
    .groupBy("job_title", F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"))
    .orderBy(F.col("hire_month").desc(), "job_title")
)
