from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.id"),
        F.col("emp.email").alias("employee_email"),
        F.col("emp.level").alias("employee_level"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.round((F.col("emp.salary") * 0.9), 2).alias("adjusted_salary"),
    )
)
