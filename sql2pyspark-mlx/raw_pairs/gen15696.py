from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%ANY%") & (F.col("dept_id") == 29) & (F.col("job_title") == "Director"))
)
