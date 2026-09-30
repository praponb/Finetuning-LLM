from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

remaining_keys = (
    df_employees
    .select("job_title")
    .exceptAll(
        df_employees
        .filter(F.col("level").isin(["L1", "L4"]))
        .select("job_title")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("full_name").rlike("^(Emma Brown|Jack Taylor)$"), "OTHER_ZONE").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "GROUP_B").otherwise("GROUP_A").alias("full_name_zone"),
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("salary").desc())
)
