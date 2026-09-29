from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.salary") <= 73000)
    .select(
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.email").alias("manager_email"),
    )
)
