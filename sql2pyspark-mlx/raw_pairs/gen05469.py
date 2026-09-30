from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("hire_date").between("2024-01-11", "2024-08-01"))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.substring("full_name", 1, 3).alias("full_name_prefix"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

result = (
    base
    .alias("s")
    .join(df_employees.alias("p"), F.col("s.manager_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["level", "employee_job_title"], ["level"], []],
        F.col("p.level").alias("level"),
        F.col("s.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("s.is_remote_flag")), 2).alias("total_is_remote_flag"),
        F.grouping_id().alias("level_id"),
    )
)
