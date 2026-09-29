from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([20, 29, 31]))
    .select("id", "dept_id", "job_title")
)
