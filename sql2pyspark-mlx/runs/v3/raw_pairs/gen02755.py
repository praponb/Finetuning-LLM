from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
)
