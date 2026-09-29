from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.dept_id") == 40)
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy("id")
)
