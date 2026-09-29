from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.email").alias("employee_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.job_title").alias("manager_job_title"),
        F.when(F.col("m.is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy("id")
)
