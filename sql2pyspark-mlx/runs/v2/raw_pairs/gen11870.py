from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.salary") >= 137000)
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.level").alias("employee_level"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
