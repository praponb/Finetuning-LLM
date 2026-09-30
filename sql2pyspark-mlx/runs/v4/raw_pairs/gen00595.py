from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.id"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.count(F.when(F.col("e.job_title") == "Manager", 1)).alias("manager_count"),
        F.max(F.col("e.hire_date")).alias("last_hire_date"),
    )
    .orderBy("last_hire_date", F.col("employee_count").desc())
)
