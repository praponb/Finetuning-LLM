from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "salary",
        "job_title",
        F.floor("salary").alias("salary_whole"),
        (F.datediff(F.current_date(), "hire_date") / 365).alias("years_since_hire_date"),
    )
    .orderBy(F.col("salary").desc(), F.col("job_title").desc())
)
