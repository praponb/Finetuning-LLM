from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "job_title",
        "full_name",
        F.initcap(F.trim("job_title")).alias("job_title_title"),
    )
)
