from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.level").isin(["L3", "L1"]) & F.col("m.full_name").like("He%"))
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.level").alias("manager_level"),
        F.year(F.col("m.hire_date")).alias("hire_date_year"),
    )
)
