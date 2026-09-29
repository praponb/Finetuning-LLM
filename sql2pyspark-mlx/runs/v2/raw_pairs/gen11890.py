from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.email").alias("employee_email"),
        F.col("m.email").alias("manager_email"),
        F.col("m.level").alias("manager_level"),
    )
)
