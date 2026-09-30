from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((~F.col("job_title").eqNullSafe(F.lit("Engineer"))) & F.col("full_name").like("F%"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

valid_employees = (
    valid
    .filter((F.col("hire_date") >= F.lit("2023-05-22").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.regexp_replace("full_name", " ", "").alias("full_name_slug"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

enriched = (
    valid_employees
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.location"), F.col("d.dept_name"))
)

valid_employees_2 = (
    enriched
    .filter(F.col("dept_id").isin([5, 12, 13, 14]))
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "full_name_slug",
        "resolved_job_title",
        "location",
        F.regexp_replace("email", " ", "-").alias("email_slug"),
        F.concat("dept_name", F.lit(" "), "location").alias("dept_name_location"),
    )
)

grouped = (
    valid_employees_2
    .groupBy("location", F.date_trunc("WEEK", "hire_date").alias("week_start"))
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
    )
    .filter(F.col("num_locations") >= 1)
)

result = (
    grouped
    .select(
        "location",
        "week_start",
        "num_locations",
        F.when(F.col("location").rlike("^(London|Bangkok)$"), "GROUP_A").otherwise("CORE").alias("location_zone"),
    )
    .orderBy(F.col("location_zone").desc())
)
