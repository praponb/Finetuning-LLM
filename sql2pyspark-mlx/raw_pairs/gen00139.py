from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter((F.col("e.job_title") != "Manager") & F.col("e.dept_id").isin([15, 17]))
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
