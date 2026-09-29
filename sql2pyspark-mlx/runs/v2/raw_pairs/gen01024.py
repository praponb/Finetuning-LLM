from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.dept_id") == 17)
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.email").alias("manager_email"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy("employee_email", F.col("manager_email").desc())
)
