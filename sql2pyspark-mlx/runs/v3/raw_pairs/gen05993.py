from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("job_title").isin(["Director", "Designer", "Intern"]))
    .select(
        "id",
        "full_name",
        F.coalesce("email", F.lit("N/A")).alias("email_filled"),
        F.coalesce("email", F.lit("unassigned")).alias("email_filled_2"),
    )
)
