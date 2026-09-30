from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.level").alias("employee_level"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
    .orderBy(F.col("employee_dept_id").desc(), "manager_hire_date")
)
