from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~F.col("m.full_name").like("Noa%") & F.col("e.dept_id").isin([38, 40]))
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.level").alias("employee_level"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.round(F.col("m.salary"), 1).alias("salary_rounded"),
    )
    .orderBy(F.col("manager_dept_id").desc(), "salary_rounded")
)
