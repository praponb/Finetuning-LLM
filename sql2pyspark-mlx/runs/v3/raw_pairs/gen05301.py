from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.level").isNull())
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy("manager_salary")
)
