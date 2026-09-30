from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.manager_id").isNotNull())
    .select(
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.email").alias("manager_email"),
    )
    .orderBy(F.col("employee_full_name").desc(), F.col("manager_email").desc())
)
