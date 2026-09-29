from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.email").alias("manager_email"),
        F.col("m.salary").alias("manager_salary"),
        F.initcap(F.trim(F.col("e.level"))).alias("level_title"),
    )
)
