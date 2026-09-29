from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.salary").alias("manager_salary"),
    )
    .orderBy("employee_full_name")
)
