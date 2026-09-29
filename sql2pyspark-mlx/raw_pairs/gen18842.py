from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.salary").alias("manager_salary"),
    )
)
