from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.email").like("%@outlook.com"))
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
