from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.salary").between(123000, 138000) & (F.col("m.job_title") == "Intern"))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.email").alias("employee_email"),
        F.col("m.email").alias("manager_email"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
