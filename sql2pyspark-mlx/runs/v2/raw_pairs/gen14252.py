from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.level").alias("employee_level"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
)
