from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.manager_id").isin([7, 8, 39, 40]))
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy(F.col("manager_level").desc())
)
