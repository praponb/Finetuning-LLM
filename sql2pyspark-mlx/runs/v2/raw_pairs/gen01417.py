from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.month("hire_date").isin([3, 6, 11]) & F.col("full_name").isNotNull() & (F.col("level") == "L1"))
    .select(
        "id",
        "salary",
        "full_name",
        F.substring("job_title", 1, 3).alias("job_title_prefix"),
        F.concat("full_name", F.lit(" "), "level").alias("full_name_level"),
    )
)
