from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("dept_id") == 14))
    .select(
        "id",
        "job_title",
        F.date_trunc("MONTH", "hire_date").alias("hire_date_period"),
    )
    .orderBy(F.col("job_title").desc(), F.col("hire_date_period").desc(), "id")
    .limit(10)
)
