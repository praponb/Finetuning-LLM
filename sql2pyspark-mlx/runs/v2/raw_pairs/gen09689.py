from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.manager_id") == 37)
    .select(
        F.col("emp.id"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.level").alias("employee_level"),
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.salary").alias("manager_salary"),
        F.when(F.col("emp.is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
    .orderBy("employee_level")
)
