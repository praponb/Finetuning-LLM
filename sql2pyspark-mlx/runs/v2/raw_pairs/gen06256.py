from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.full_name").like("Em%") & (F.col("m.hire_date") >= "2024-10-01"))
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
)
