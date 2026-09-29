from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("m.hire_date") < "2025-02-04") & (F.length(F.col("m.email")) > 8))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.level").alias("employee_level"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.level").alias("manager_level"),
    )
)
