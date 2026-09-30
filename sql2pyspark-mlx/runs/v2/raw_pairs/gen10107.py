from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.manager_id") == 12)
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
