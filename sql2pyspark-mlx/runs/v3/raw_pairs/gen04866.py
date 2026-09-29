from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.lower(F.trim(F.col("m.full_name"))) == "alice wong") | F.col("m.email").isNotNull())
    .select(
        F.col("e.level").alias("employee_level"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.email").alias("manager_email"),
        F.col("m.level").alias("manager_level"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
