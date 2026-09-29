from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.manager_id").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.email").alias("employee_email"),
        F.col("m.salary").alias("manager_salary"),
    )
)
