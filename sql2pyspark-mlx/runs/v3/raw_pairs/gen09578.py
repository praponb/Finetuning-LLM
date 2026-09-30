from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("emp.dept_id").isin([22, 33, 40]))
    .select(
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.email").alias("manager_email"),
    )
    .orderBy("employee_hire_date")
)
