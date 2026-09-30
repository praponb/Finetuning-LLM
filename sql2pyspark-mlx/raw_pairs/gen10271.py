from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") == "Director")
    .select(
        "manager_id",
        "level",
        "is_remote",
        F.coalesce("job_title", F.lit("Unknown")).alias("job_title_filled"),
    )
    .orderBy(F.col("manager_id").desc())
)
