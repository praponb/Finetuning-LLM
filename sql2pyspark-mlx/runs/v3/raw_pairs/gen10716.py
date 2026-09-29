from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.upper(F.col("e.full_name")).like("%OAH%") & F.upper(F.col("e.full_name")).like("%ACK%"))
    .select(
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
    .orderBy(F.col("employee_manager_id").desc(), F.col("manager_salary").desc())
)
