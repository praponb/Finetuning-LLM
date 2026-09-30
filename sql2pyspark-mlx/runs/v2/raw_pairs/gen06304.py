from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(F.col("email").like("%@gmail.com") & F.col("full_name").isNotNull() & F.col("manager_id").isin([25, 30]))
)

employees_stats = (
    valid_employees
    .groupBy("job_title", F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"))
)

result = employees_stats.select("job_title", "l4_count")
