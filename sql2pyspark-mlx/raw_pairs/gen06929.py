from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~(F.col("m.level").isin(["L4", "L1"]) & F.lower(F.col("m.email")).like("%iam%")))
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.salary").alias("manager_salary"),
        F.concat_ws("-", F.col("m.job_title"), F.col("m.full_name")).alias("job_title_full_name_key"),
    )
)
