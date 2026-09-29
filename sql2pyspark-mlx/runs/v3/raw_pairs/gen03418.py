from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.email").alias("employee_email"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy(F.col("employee_salary").desc(), "id")
    .limit(20)
)
