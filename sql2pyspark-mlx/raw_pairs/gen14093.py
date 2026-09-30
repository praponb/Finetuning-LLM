from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.email").isNull() | (F.col("emp.salary") < 91000))
    .select(
        F.col("emp.email").alias("employee_email"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.email").alias("manager_email"),
    )
    .orderBy(F.col("manager_manager_id").desc())
)
