from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([4, 38]))
    .select("hire_date")
    .intersect(
        df_employees
        .filter(F.col("job_title") == "Engineer")
        .select("hire_date")
    )
)
