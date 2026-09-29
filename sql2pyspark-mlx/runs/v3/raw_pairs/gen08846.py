from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.full_name").like("Ka%") | F.col("m.dept_id").isin([5, 22]))
    .select(
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.level").alias("employee_level"),
        F.col("m.salary").alias("manager_salary"),
    )
    .orderBy("employee_level")
)
