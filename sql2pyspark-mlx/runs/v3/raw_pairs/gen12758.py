from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.level").isin(["L4", "L1", "L5"]))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.email").alias("employee_email"),
        F.col("m.email").alias("manager_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
