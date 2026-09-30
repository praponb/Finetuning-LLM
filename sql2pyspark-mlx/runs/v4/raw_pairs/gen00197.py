from pyspark.sql import functions as F

result = (
    df_employees
    .select("job_title")
    .distinct()
    .orderBy("job_title")
    .limit(1)
)
