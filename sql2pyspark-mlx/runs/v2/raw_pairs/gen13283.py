from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("m.level") == "L1") & ~F.col("e.job_title").isin(["Manager", "Director", "Engineer"]))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.email").alias("manager_email"),
    )
    .orderBy(F.col("employee_manager_id").desc(), "id")
    .limit(3)
)
