from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.full_name").like("Ol%") | F.col("m.dept_id").isin([4, 7]))
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.email").alias("employee_email"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
