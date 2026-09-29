from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.email").alias("employee_email"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
)
