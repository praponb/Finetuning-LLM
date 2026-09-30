from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.job_title").isin(["Manager", "Intern"]))
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.full_name").alias("manager_full_name"),
    )
    .orderBy(F.col("employee_manager_id").desc(), "id")
    .limit(20)
)
