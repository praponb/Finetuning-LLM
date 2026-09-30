from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.email").like("%@outlook.com") & F.col("e.full_name").like("%Chaiy%"))
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.level").alias("manager_level"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
