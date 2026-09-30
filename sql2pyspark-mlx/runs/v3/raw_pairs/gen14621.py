from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNull() & F.col("hire_date").between("2023-10-06", "2023-10-20"))
    .select(
        "id",
        "hire_date",
        "email",
        F.upper("full_name").alias("full_name_upper"),
        F.length("job_title").alias("job_title_length"),
    )
    .orderBy(F.col("email").desc())
)
