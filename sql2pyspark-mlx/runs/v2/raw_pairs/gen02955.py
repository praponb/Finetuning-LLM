from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.job_title").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy(F.col("manager_job_title").desc(), "id")
    .limit(20)
)
