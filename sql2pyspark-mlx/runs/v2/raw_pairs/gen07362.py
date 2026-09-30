from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.salary") > 68000)
    .select(
        F.col("emp.id"),
        F.col("emp.email").alias("employee_email"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.when(F.col("mgr.level").isin(["L1", "L3"]), "Priority").otherwise("Other").alias("level_group"),
    )
    .orderBy(F.col("employee_manager_id").desc(), F.col("level_group").desc())
)
