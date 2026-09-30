from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.manager_id") == 22)
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.job_title").alias("manager_job_title"),
        F.when(F.col("e.job_title") == "Analyst", 1).otherwise(0).alias("is_analyst"),
    )
    .orderBy(F.col("id").desc())
)
