from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
)
