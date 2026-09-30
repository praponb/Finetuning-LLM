from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-02-16"))) & F.col("dept_id").isin([15, 29, 38]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.concat_ws(",", "email", "level").alias("email_level_key"),
        F.length("full_name").alias("full_name_length"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.full_name",
        "a.dept_id",
        "a.job_title",
        "a.is_remote",
        "a.level",
        "a.email_level_key",
        "a.full_name_length",
        F.col("d.dept_name"),
        F.col("d.location"),
    )
)

enriched_employees = (
    enriched
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.dept_id",
        "e.job_title",
        "e.full_name_length",
        "e.location",
        F.col("d.dept_name").alias("department_dept_name"),
        F.col("d.location").alias("department_location"),
    )
)

joined = (
    enriched_employees
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.full_name",
        "src.dept_id",
        "src.full_name_length",
        "src.department_dept_name",
        "src.department_location",
        F.col("b.location").alias("department_location_2"),
    )
)

result = (
    joined
    .groupingSets(
        [["department_dept_name", "department_location_2"], ["department_dept_name"], []],
        "department_dept_name",
        "department_location_2",
    )
    .agg(
        F.round(F.sum("full_name_length") / F.nullif(F.count(F.when(F.col("department_location") == "Tokyo", 1)), F.lit(0)), 2).alias("full_name_length_per_tokyo"),
        F.count(F.when(F.col("department_location") == "Bangkok", 1)).alias("bangkok_count"),
        F.grouping("department_dept_name").alias("gid"),
    )
)
