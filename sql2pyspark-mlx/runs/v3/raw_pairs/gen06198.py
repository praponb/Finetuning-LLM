from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "hire_date",
        "manager_id",
        "salary",
        F.coalesce("job_title", F.lit("unassigned")).alias("job_title_filled"),
    )
)
