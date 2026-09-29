from pyspark.sql import functions as F

salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

with_dims = (
    df_employees
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "inner")
    .filter(F.col("b.manager_id").isin([22, 26, 38]))
    .select(
        "s.*",
        F.col("b.hire_date").alias("employee_hire_date"),
        F.col("b.level").alias("employee_level"),
        F.col("b.job_title").alias("employee_job_title"),
    )
)

result = (
    with_dims
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
        F.col("max_employee_salary"),
    )
)
