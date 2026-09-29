from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy(F.col("manager_hire_date").desc(), "employee_dept_id")
)
