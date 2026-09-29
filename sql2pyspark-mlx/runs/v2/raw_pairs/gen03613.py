from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "dept_id",
        F.coalesce("job_title", F.lit("unassigned")).alias("job_title_filled"),
    )
)
