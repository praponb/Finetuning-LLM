from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

remaining_keys = (
    df_employees
    .select("manager_id", "level")
    .subtract(
        df_employees
        .filter(F.col("job_title").isin(["Intern", "Engineer"]))
        .select("manager_id", "level")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("full_name").rlike("^(Pim Chaiyaporn|Henry Ford)$"), "PRIORITY").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("full_name_zone"),
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("max_employee_salary").desc(), "hire_date")
)
