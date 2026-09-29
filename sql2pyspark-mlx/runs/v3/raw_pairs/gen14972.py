from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.is_remote") & F.col("m.job_title").isin(["Manager", "Intern"]))
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
