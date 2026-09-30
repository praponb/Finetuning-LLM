from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.email").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.email").alias("manager_email"),
    )
    .orderBy("manager_email", "id")
    .limit(5)
)
