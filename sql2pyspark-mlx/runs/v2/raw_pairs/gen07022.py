from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.salary").alias("manager_salary"),
    )
    .orderBy(F.col("employee_level").desc())
)
