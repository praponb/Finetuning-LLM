from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.lower(F.col("e.email")).like("%iam%"))
    .select(
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
)
