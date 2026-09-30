from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-02-28")))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        F.trim("job_title").alias("clean_job_title"),
    )
)

result = (
    cleaned
    .select("level", "id")
    .exceptAll(
        df_employees
        .filter(F.col("level") == "L5")
        .select("level", "id")
    )
)
