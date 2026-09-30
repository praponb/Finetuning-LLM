from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") >= "2025-07-26") & (F.col("job_title") != "Director"))
    .select(
        "hire_date",
        "salary",
        "full_name",
        F.substring("level", 1, 2).alias("level_prefix"),
    )
    .orderBy("level_prefix", "hire_date")
)
