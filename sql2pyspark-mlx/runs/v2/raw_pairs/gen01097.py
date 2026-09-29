from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNull())
    .select("email", "job_title")
)
