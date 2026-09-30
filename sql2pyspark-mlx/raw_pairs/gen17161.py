from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter((F.col("job_title").eqNullSafe(F.lit("Engineer"))) & (F.col("job_title") != "Intern"))
    .select(
        "*",
        F.lower("full_name").alias("full_name_lower"),
        F.coalesce("job_title", F.lit("OTHER")).alias("resolved_job_title"),
    )
)

matched_keys = (
    filtered
    .select("manager_id", "level")
    .intersect(
        df_employees
        .filter(F.col("job_title") == "Intern")
        .select("manager_id", "level")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget") < 3629000)
    .select("e.*", F.col("d.budget"))
)

filtered_employees = (
    joined
    .filter(F.col("level") == "L4")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        "full_name_lower",
        "resolved_job_title",
        "budget",
        F.coalesce("job_title", F.lit("unassigned")).alias("job_title_filled"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_filled_2"),
    )
)

result = (
    filtered_employees
    .select(
        "id",
        "full_name",
        "manager_id",
        "hire_date",
        "is_remote",
        "full_name_lower",
        "resolved_job_title",
        "budget",
        "job_title_filled_2",
    )
    .orderBy(F.col("budget").desc())
)
