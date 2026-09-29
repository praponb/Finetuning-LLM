from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.manager_id").isNull())
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy("manager_dept_id", "employee_hire_date")
)
