from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.year(F.col("m.hire_date")) >= 2025) & F.col("e.full_name").like("Noa%"))
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.email").alias("manager_email"),
        F.col("m.full_name").alias("manager_full_name"),
        F.length(F.col("e.email")).alias("email_length"),
    )
)
