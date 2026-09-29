from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "job_title",
        "level",
        F.concat("email", F.lit(" "), "full_name").alias("email_full_name"),
        F.coalesce("full_name", F.lit("unassigned")).alias("full_name_filled"),
    )
    .orderBy("email_full_name", "job_title")
)
