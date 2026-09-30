from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.upper(F.trim(F.col("mgr.email"))).alias("email_normalized"),
    )
    .orderBy("email_normalized", F.col("manager_manager_id").desc())
)
