from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.email").alias("manager_email"),
        F.col("m.level").alias("manager_level"),
    )
)
