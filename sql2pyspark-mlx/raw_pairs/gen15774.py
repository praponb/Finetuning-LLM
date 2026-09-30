from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.dept_id").isin([12, 27, 34, 38]))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
    .orderBy(F.col("manager_full_name").desc())
)
