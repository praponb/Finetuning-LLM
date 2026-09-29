from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.level").isin(["L2", "L5"]) | F.col("e.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
)
