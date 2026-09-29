from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.manager_id").isin([16, 28]) & F.col("e.email").isNotNull())
    .select(
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.full_name").alias("manager_full_name"),
    )
    .orderBy(F.col("employee_full_name").desc())
)
