from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & F.col("full_name").like("No%"))
    .select(
        "id",
        "job_title",
        "hire_date",
        "is_remote",
        F.upper("level").alias("level_upper"),
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
)
