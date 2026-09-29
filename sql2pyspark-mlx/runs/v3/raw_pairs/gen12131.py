from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("m.job_title") == "Analyst") | F.col("m.job_title").isin(["Director", "Designer"]))
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.email").alias("employee_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.coalesce(F.col("e.job_title"), F.lit("N/A")).alias("job_title_filled"),
    )
    .orderBy("job_title_filled")
)
