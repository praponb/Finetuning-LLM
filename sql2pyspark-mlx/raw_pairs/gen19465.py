from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-02-08").cast("date")) & (F.col("job_title").eqNullSafe(F.lit("Director"))))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        F.round("salary", 1).alias("salary_rounded"),
        F.regexp_replace("full_name", " ", "").alias("full_name_slug"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_filled"),
    )
)

result = (
    valid
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.full_name",
        "a.email",
        "a.dept_id",
        "a.manager_id",
        "a.job_title",
        "a.job_title_filled",
        F.col("d.dept_name"),
    )
)
