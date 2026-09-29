from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower("full_name").like("%ob %"))
    .select("id", "email", "job_title")
    .subtract(
        df_employees
        .filter(F.col("job_title") == "Director")
        .select("id", "email", "job_title")
    )
    .orderBy(F.col("id").desc())
)
