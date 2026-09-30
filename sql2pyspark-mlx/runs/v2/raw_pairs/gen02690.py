from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Manager")
    .select(
        "dept_id",
        "email",
        F.concat("full_name", F.lit(" - "), "email").alias("full_name_email"),
    )
)
