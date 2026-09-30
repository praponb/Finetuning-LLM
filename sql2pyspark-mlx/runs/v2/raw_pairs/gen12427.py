from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.full_name").like("Mi%"))
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy(F.col("manager_dept_id").desc(), F.col("id").desc())
)
