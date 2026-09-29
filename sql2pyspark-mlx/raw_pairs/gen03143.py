from pyspark.sql import functions as F

salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

enriched = (
    df_employees
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "src.id",
        "src.dept_id",
        "src.manager_id",
        "src.job_title",
        "src.hire_date",
        "src.level",
        F.col("d.job_title").alias("employee_job_title"),
        F.col("d.level").alias("employee_level"),
    )
)

result = (
    enriched
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        "employee_job_title",
        "employee_level",
        F.col("avg_employee_salary"),
    )
    .orderBy(F.col("avg_employee_salary").desc())
)
