from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.manager_id").isNotNull() | ~F.col("e.job_title").isin(["Manager", "Director"]))
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
)
