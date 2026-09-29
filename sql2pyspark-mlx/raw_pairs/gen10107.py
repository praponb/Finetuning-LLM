from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.hire_date").between("2024-12-01", "2025-06-09"))
    .select(
        F.col("e.level").alias("employee_level"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
    .orderBy(F.col("manager_manager_id").desc())
)
