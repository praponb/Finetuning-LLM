from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.dept_id").isin([14, 20]))
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("e.salary").cast("int").alias("salary_int"),
    )
    .orderBy("employee_salary", "id")
    .limit(5)
)
