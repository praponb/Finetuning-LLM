from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-09-08"))) & F.col("job_title").isNotNull())
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
        F.date_trunc("week", "hire_date").alias("hire_date_period"),
        F.concat("full_name", F.lit(" "), "level").alias("full_name_level"),
        F.coalesce("level", F.lit("NONE")).alias("level_clean"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .filter(~F.col("e.is_remote"))
    .select(
        "s.id",
        "s.full_name",
        "s.manager_id",
        "s.job_title",
        "s.salary",
        "s.hire_date",
        "s.is_remote",
        "s.level_clean",
        F.col("e.email").alias("employee_email"),
        F.col("e.hire_date").alias("employee_hire_date"),
    )
)
