from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.job_title").isin(["Director", "Engineer"]))
    .select(
        F.col("emp.id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.full_name").alias("manager_full_name"),
    )
)
