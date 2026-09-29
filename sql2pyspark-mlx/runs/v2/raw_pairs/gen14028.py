from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(~F.col("emp.email").like("%@startup.io") | F.col("emp.manager_id").isNull())
    .select(
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
    )
    .orderBy(F.col("manager_salary").desc())
)
