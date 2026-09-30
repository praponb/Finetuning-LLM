from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
