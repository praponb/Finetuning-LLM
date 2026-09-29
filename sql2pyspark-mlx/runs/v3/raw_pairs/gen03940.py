from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(~F.col("emp.full_name").like("K%"))
    .select(
        F.col("emp.id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.level").alias("manager_level"),
        F.upper(F.col("mgr.level")).alias("level_upper"),
    )
    .orderBy("level_upper")
)
