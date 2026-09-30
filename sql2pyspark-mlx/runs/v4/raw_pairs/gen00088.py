from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.level") != "L3")
    .select(
        F.col("emp.id"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
    )
    .orderBy(F.col("manager_dept_id").desc(), "manager_hire_date")
)
