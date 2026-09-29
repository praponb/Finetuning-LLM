from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.count(F.when(F.col("e.job_title") == "Manager", 1)).alias("manager_count"),
        F.sum(F.when(F.col("e.job_title") == "Designer", F.col("e.salary")).otherwise(0)).alias("designer_salary"),
    )
)
