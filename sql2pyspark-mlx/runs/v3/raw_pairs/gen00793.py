from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.manager_id").isNotNull() & (F.col("m.hire_date") < "2024-07-21"))
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.email").alias("manager_email"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.level").alias("manager_level"),
        F.col("e.is_remote").cast("int").alias("is_remote_flag"),
    )
)
