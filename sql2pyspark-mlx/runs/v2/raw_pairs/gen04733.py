from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 141000)
    .select("job_title", "dept_id")
    .unionByName(
        df_employees
        .filter(F.col("full_name").like("Gra%"))
        .select("job_title", "dept_id")
    )
)
