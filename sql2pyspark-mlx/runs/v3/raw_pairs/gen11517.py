from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.lower(F.col("m.email")).like("%iam%") | F.col("e.job_title").isin(["Analyst", "Engineer", "Designer"]))
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
