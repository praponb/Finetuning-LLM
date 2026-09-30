from pyspark.sql import functions as F

rollup_totals = (
    df_employees
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["employee_job_title", "employee_job_title"], ["employee_job_title"], []],
        F.col("d.job_title").alias("employee_job_title"),
        F.col("s.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.salary")).alias("max_salary"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    rollup_totals
    .select(
        "employee_job_title",
        "employee_job_title",
        "rows_total",
        "max_salary",
        "grouping_level",
        F.when(F.col("employee_job_title").rlike("^(Analyst|Manager)$"), "EXTENDED").when(F.col("employee_job_title").rlike("^(Engineer|Director)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("employee_job_title_zone"),
    )
    .orderBy("employee_job_title", "max_salary")
)
