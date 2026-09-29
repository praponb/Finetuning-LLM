from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-07-09").cast("date")) & (F.col("salary") <= 79000))
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.initcap(F.trim("full_name")).alias("full_name_title"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "e.id",
        "e.manager_id",
        "e.is_remote",
        "e.level",
        "e.full_name_title",
        F.col("p.job_title").alias("employee_job_title"),
    )
)
