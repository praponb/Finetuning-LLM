from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("K%"))
    .select(
        "id",
        "job_title",
        "email",
        "level",
        F.concat("email", F.lit(" "), "level").alias("email_level"),
    )
)
