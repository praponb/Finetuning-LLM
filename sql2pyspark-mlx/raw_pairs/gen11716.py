from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-05-24"))) & (F.col("hire_date") >= "2025-07-01"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.lower("full_name").alias("full_name_lower"),
        F.coalesce("job_title", F.lit("OTHER")).alias("job_title_clean"),
    )
)

result = (
    base
    .groupBy("dept_id")
    .pivot("job_title", ["Director", "Designer", "Engineer"])
    .agg(F.max("salary"))
)
