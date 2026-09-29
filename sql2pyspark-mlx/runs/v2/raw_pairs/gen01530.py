from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNull())
    .select("id", "salary", "hire_date", "job_title")
    .orderBy("id")
)
