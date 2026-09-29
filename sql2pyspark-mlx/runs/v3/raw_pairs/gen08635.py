from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select("dept_id", "job_title")
    .subtract(
        df_employees
        .filter(~F.col("is_remote"))
        .select("dept_id", "job_title")
    )
)
