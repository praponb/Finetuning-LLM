from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.level") == "L4")
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.salary").alias("manager_salary"),
    )
    .orderBy(F.col("manager_salary").desc())
)
