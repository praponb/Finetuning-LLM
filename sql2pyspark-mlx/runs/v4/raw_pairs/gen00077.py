from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.avg("salary").alias("avg_employee_salary"))

joined = (
    df_employees
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "inner")
    .select(
        "src.*",
        F.col("e.email").alias("employee_email"),
        F.col("e.level").alias("employee_level"),
    )
)

result = (
    joined
    .join(salary_by_manager_id, on="manager_id", how="left")
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
        F.col("avg_employee_salary"),
    )
    .orderBy(F.col("job_title").asc_nulls_last(), "manager_id")
)
