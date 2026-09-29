from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.job_title").isin(["Director", "Analyst", "Designer"]) & F.upper(F.col("mgr.full_name")).like("%IA %"))
    .select(
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.level").alias("manager_level"),
        F.date_format(F.col("emp.hire_date"), "yyyy-MM-dd").alias("hire_date_label"),
    )
)
