from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter(F.col("job_title").isin(["Analyst", "Designer"]) | ~F.col("job_title").isin(["Manager", "Designer"]))
)

employees_stats = (
    filtered_employees
    .groupBy("is_remote", F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(F.max("hire_date").alias("last_hire_date"))
)

result = (
    employees_stats
    .select("is_remote", "hire_month", "last_hire_date")
    .orderBy(F.col("hire_month").desc(), "last_hire_date")
)
