from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
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
        F.concat("email", F.lit(" - "), "level").alias("email_level"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

base = (
    cleaned
    .filter(F.col("job_title").isNotNull() & F.col("full_name").like("%Wong%"))
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level_filled",
        F.substring("job_title", 1, 2).alias("job_title_prefix"),
    )
)

matched_keys = (
    base
    .select("job_title")
    .subtract(
        df_employees
        .filter(F.col("manager_id").isNull())
        .select("job_title")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.budget"))
)

result = (
    with_dims
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.dept_name").alias("dept_name"),
        F.col("src.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.salary")).alias("max_salary"),
    )
    .orderBy("max_salary")
)
