from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~F.col("e.is_remote") & F.col("m.full_name").like("A%"))
    .select(
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.email").alias("manager_email"),
        F.col("m.level").alias("manager_level"),
    )
)
