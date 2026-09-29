from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.dept_id").isin([10, 22, 29, 38]) & F.col("m.email").like("%@GMAIL.COM"))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.email").alias("manager_email"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
