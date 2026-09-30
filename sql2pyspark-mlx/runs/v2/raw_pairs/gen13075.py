from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("e.level") == "L4") | F.col("e.full_name").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.email").alias("employee_email"),
        F.col("m.salary").alias("manager_salary"),
        F.coalesce(F.col("e.job_title"), F.lit("unassigned")).alias("job_title_filled"),
    )
)
