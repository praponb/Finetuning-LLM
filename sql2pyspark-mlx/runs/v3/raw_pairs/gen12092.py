from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.email").like("%@gmail.com"))
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.full_name").alias("manager_full_name"),
        F.round((F.col("e.salary") * 0.85), 2).alias("adjusted_salary"),
    )
)
