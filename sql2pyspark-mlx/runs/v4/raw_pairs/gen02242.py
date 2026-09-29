from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.job_title").isin(["Manager", "Intern", "Engineer"]))
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.level").alias("manager_level"),
    )
)
