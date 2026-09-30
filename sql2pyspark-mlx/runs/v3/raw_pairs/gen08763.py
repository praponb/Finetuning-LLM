from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~((F.col("m.salary") > 59000) & (F.col("e.salary") < 100000)))
    .select(
        F.col("e.id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy("employee_full_name")
)
