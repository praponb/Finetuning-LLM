from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.hire_date").between("2023-10-25", "2025-03-01") & (F.col("e.job_title") == "Analyst"))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.full_name").alias("manager_full_name"),
        F.substring(F.col("m.job_title"), 1, 3).alias("job_title_prefix"),
    )
)
