from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.hire_date") > "2024-07-01")
    .select(
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.email").alias("manager_email"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
    .orderBy("employee_dept_id")
)
