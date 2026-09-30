from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("e.level") == "L1") | F.col("m.job_title").isin(["Intern", "Director", "Manager"]))
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.email").alias("manager_email"),
    )
)
