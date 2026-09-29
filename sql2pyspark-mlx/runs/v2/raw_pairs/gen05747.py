from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "level",
        "email",
        F.lower("job_title").alias("job_title_lower"),
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)
