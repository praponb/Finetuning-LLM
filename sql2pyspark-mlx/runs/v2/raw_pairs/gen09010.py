from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.salary").between(73000, 111000) | (F.col("e.job_title") == "Analyst"))
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
