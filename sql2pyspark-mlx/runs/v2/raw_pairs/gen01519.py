from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.full_name").like("J%"))
    .select(
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.salary").alias("manager_salary"),
        F.last_day(F.col("emp.hire_date")).alias("hire_date_month_end"),
    )
    .orderBy(F.col("employee_hire_date").desc())
)
