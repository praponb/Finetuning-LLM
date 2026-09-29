from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.is_remote") & F.col("m.manager_id").isNull())
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy(F.col("employee_level").desc())
)
