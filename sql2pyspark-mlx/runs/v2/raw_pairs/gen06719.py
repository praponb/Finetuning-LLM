from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.dept_id") == 14)
    .select(
        F.col("e.id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
    .orderBy("manager_hire_date", "id")
    .limit(3)
)
