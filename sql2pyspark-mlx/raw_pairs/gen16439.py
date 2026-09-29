from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.lower(F.trim(F.col("m.email"))) == "jack@startup.io")
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.email").alias("employee_email"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
