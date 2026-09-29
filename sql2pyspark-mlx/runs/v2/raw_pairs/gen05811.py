from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.level").alias("manager_level"),
        F.col("m.email").alias("manager_email"),
    )
    .orderBy(F.col("employee_manager_id").desc(), "manager_email")
)
