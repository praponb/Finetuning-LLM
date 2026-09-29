from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([3, 4, 15, 34]))
    .select("job_title")
    .distinct()
    .orderBy("job_title")
)
