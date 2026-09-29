from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("e.level") == "L1") & ~F.col("m.job_title").isin(["Director", "Manager"]))
    .select(
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
    .orderBy("manager_hire_date")
)
