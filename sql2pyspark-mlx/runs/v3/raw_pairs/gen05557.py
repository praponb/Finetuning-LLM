from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("m.salary") < 77000) & (F.col("e.level") == "L2"))
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
