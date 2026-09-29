from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.email").like("%@startup.io"))
    .select(
        F.col("e.level").alias("employee_level"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
