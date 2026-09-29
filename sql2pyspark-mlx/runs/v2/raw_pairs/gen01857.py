from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.manager_id").isNull() & F.col("e.manager_id").isin([11, 16, 21, 24]))
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.level").alias("manager_level"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.email").alias("manager_email"),
    )
)
