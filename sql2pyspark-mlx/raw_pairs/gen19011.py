from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([10, 37]))
    .select("id", "job_title", "hire_date")
    .unionByName(
        df_employees
        .filter(F.col("dept_id").isin([5, 7, 10]))
        .select("id", "job_title", "hire_date")
    )
)
