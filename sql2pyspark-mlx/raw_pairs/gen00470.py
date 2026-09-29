from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.salary").between(71000, 110000))
    .select(
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.level").alias("employee_level"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.level").alias("manager_level"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.job_title").alias("manager_job_title"),
    )
)
