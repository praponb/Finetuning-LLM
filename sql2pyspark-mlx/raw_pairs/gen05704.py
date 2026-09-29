from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.hire_date").between("2023-09-11", "2023-12-01") | F.col("m.is_remote"))
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
    .orderBy("manager_job_title")
)
