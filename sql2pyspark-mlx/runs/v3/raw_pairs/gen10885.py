from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.full_name").like("Em%") | F.col("m.hire_date").between("2023-12-20", "2024-08-14"))
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.email").alias("manager_email"),
        F.col("m.full_name").alias("manager_full_name"),
    )
    .orderBy("employee_manager_id", "id")
    .limit(5)
)
