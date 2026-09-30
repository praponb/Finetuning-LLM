from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.job_title").isin(["Analyst", "Director", "Designer"]) & (F.col("m.level") != "L1"))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy("manager_level")
)
