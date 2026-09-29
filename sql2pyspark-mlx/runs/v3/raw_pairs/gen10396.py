from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2023-08-01")
    .select(
        "id",
        "full_name",
        "job_title",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
    .orderBy(F.col("job_title").desc(), "full_name", "id")
    .limit(20)
)
