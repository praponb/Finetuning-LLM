from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.avg("salary").alias("avg_employee_salary"))

with_dims = (
    df_employees
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "inner")
    .select(
        "src.id",
        "src.full_name",
        "src.dept_id",
        "src.manager_id",
        "src.job_title",
        "src.is_remote",
        F.col("e.email").alias("employee_email"),
    )
)

result = (
    with_dims
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "is_remote",
        "employee_email",
        F.col("avg_employee_salary"),
    )
)
