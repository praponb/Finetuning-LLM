from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.is_remote") & F.col("m.manager_id").isin([3, 12, 19, 29]))
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy(F.col("employee_full_name").desc())
)
