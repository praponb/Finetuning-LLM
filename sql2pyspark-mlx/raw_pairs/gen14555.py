from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.level").alias("manager_level"),
        F.col("m.salary").alias("manager_salary"),
    )
)
