from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Intern")
    .select("id", "salary", F.add_months("hire_date", 3).alias("hire_date_plus_months"))
    .orderBy(F.col("salary").desc())
)
