from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter((F.col("m.salary") <= 112000) & F.col("e.email").like("%@gmail.com"))
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
    .orderBy(F.col("manager_manager_id").desc())
)
