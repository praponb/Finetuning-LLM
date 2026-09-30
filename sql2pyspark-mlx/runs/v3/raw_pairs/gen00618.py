from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.full_name").like("%Carol%"))
    .select(
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
