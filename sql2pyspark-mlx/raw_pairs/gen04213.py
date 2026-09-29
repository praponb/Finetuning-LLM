from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter((F.col("e.manager_id") == 21) & F.col("e.full_name").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
