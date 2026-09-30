from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level") == "L1")
    .select(
        "job_title",
        "manager_id",
        F.coalesce("level", F.lit("Unknown")).alias("level_filled"),
        F.trim("full_name").alias("clean_full_name"),
    )
)
