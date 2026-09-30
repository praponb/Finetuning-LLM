from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("m.job_title") == "Manager") & ~F.col("m.full_name").like("%Emma%"))
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("m.full_name").alias("manager_full_name"),
    )
    .orderBy(F.col("manager_full_name").desc(), "employee_email")
)
