from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.hire_date").between("2023-06-13", "2024-09-01") & F.col("m.email").like("%@startup.io"))
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
