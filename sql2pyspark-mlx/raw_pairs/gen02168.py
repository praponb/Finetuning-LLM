from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.job_title").isin(["Engineer", "Designer"]))
    .select(
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
    )
)
