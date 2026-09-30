from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.level").alias("manager_level"),
        F.col("m.email").alias("manager_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
