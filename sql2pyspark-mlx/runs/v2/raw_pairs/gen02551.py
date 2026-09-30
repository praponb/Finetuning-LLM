from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter((F.col("mgr.salary") >= 144000) & ~F.col("emp.job_title").isin(["Manager", "Analyst", "Engineer"]))
    .select(
        F.col("emp.id"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.email").alias("manager_email"),
        F.date_trunc("week", F.col("mgr.hire_date")).alias("hire_date_period"),
    )
    .orderBy(F.col("employee_salary").desc())
)
