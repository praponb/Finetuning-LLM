from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.manager_id").isNull() & F.col("m.salary").between(86000, 141000))
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
