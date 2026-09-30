from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([14, 17, 30, 38]))
    .select(
        "id",
        "salary",
        "full_name",
        "hire_date",
        F.length("job_title").alias("job_title_length"),
        F.concat_ws("-", "full_name", "level").alias("full_name_level_key"),
    )
    .orderBy(F.col("job_title_length").desc(), "id")
    .limit(5)
)
