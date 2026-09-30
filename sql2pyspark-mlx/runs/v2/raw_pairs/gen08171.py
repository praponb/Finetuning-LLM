from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.salary") <= 107000)
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
)
