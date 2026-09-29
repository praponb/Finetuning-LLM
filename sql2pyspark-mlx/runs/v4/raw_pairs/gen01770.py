from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.level").isin(["L3", "L4", "L5"]))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.email").alias("manager_email"),
    )
)
