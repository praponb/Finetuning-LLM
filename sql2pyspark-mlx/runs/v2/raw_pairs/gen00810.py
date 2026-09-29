from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.hire_date") < "2025-03-01")
    .select(
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy(F.col("employee_dept_id").desc())
)
