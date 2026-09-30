from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.add_months(F.col("e.hire_date"), 6).alias("hire_date_plus_months"),
    )
)
