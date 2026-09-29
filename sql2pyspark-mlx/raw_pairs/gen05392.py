from pyspark.sql import functions as F

dim_totals = (
    df_employees
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "inner")
    .groupingSets(
        [["employee_job_title", "employee_level"], ["employee_job_title"]],
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.salary")).alias("min_salary"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    dim_totals
    .select(
        "employee_job_title",
        "employee_level",
        "min_salary",
        "level_id",
        F.when(F.col("employee_job_title").rlike("^(Manager|Analyst)$"), "GROUP_B").when(F.col("employee_job_title").rlike("^(Engineer|Director)$"), "CORE").otherwise("GROUP_A").alias("employee_job_title_zone"),
    )
    .orderBy(F.col("employee_job_title_zone").desc_nulls_last(), "employee_job_title")
)
