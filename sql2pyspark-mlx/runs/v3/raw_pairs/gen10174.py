from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 13)
    .select("job_title")
    .distinct()
)
