from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 8) | (F.col("job_title") == "Manager"))
    .select(
        "id",
        "salary",
        F.substring("level", 1, 2).alias("level_prefix"),
        F.concat("job_title", F.lit(" - "), "level").alias("job_title_level"),
    )
)
