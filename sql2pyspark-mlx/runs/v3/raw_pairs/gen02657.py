from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.level") != "L3")
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.level").alias("manager_level"),
    )
)
