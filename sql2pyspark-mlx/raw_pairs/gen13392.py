from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .select(
        "id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
        F.concat("job_title", F.lit(" - "), "email").alias("job_title_email"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .filter(~F.col("e.job_title").isin(["Manager", "Analyst"]))
    .select(
        "src.id",
        "src.manager_id",
        "src.job_title",
        "src.job_title_email",
        F.col("e.full_name"),
        F.col("e.level").alias("employee_level"),
        F.col("e.job_title").alias("employee_job_title"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.manager_id",
        "src.job_title",
        "src.full_name",
        "src.employee_level",
        F.col("d.email"),
        F.col("d.level"),
    )
)

kept_keys = (
    with_dims
    .select("manager_id", "id")
    .intersect(
        df_employees
        .filter(F.col("hire_date").between("2024-03-01", "2024-09-01"))
        .select("manager_id", "id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined_employees = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .select(
        "s.*",
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.hire_date"),
        F.col("e.email").alias("employee_email"),
    )
)

joined_employees_2 = (
    joined_employees
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "e.manager_id",
        "e.job_title",
        "e.full_name",
        "e.employee_level",
        "e.level",
        "e.employee_full_name",
        "e.hire_date",
        "e.employee_email",
        F.col("p.level").alias("employee_level_2"),
    )
)

result = (
    joined_employees_2
    .select(
        "manager_id",
        "job_title",
        "full_name",
        "employee_level",
        "level",
        "employee_full_name",
        "hire_date",
        "employee_email",
    )
    .orderBy(F.col("level").desc_nulls_last(), "job_title")
)
