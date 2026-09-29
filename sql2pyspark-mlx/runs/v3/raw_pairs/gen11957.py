from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.level").isin(["L3", "L2", "L4"]))
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.email").alias("manager_email"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.month(F.col("m.hire_date")).alias("hire_date_month"),
    )
    .orderBy(F.col("id").desc())
)
