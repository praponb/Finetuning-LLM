from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@startup.io") | F.col("email").like("%@outlook.com"))
    .select("job_title")
)
