from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

matched_keys = (
    df_employees
    .select("dept_id", "level")
    .exceptAll(
        df_employees
        .filter(F.col("manager_id").isin([12, 24, 26]))
        .select("dept_id", "level")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "is_remote",
        F.when(F.col("full_name").rlike("^(Alice Wong|Olivia Garcia)$"), "CORE").otherwise("PRIORITY").alias("full_name_zone"),
        F.col("avg_employee_salary"),
    )
)
