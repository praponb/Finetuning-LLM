from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("email").like("%@outlook.com"))
    .select(
        "level",
        "full_name",
        "job_title",
        F.concat("full_name", F.lit(" - "), "job_title").alias("full_name_job_title"),
    )
)
