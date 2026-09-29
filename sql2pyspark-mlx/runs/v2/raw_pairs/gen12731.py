from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~(F.col("m.salary").between(121000, 132000) & (F.length(F.col("e.full_name")) < 8)))
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.level").alias("employee_level"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy(F.col("employee_level").desc(), "employee_full_name")
)
