from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.full_name").isNotNull())
    .select(
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
