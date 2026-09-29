from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.manager_id").isNotNull())
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
)
