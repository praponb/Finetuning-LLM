from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.email").like("%@yahoo.com") & F.col("emp.full_name").isNotNull())
    .select(
        F.col("emp.salary").alias("employee_salary"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
    )
    .orderBy(F.col("manager_dept_id").desc(), F.col("employee_salary").desc())
)
