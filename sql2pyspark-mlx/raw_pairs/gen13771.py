from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.year(F.col("e.hire_date")) == 2025)
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.email").alias("employee_email"),
        F.col("m.email").alias("manager_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy("employee_email", F.col("id").desc())
)
