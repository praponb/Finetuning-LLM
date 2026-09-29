from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(82000, 94000))
    .select(
        "id",
        "email",
        "hire_date",
        "job_title",
        F.substring("job_title", 1, 3).alias("job_title_prefix"),
    )
    .orderBy("id")
)
