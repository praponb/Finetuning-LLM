from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select("job_title")
    .distinct()
)
