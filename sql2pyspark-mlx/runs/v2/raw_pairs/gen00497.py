from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.email").isNotNull())
    .select(
        F.col("e.level").alias("employee_level"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.email").alias("manager_email"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
    .orderBy("manager_email")
)
