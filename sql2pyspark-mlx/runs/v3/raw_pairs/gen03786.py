from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.salary").between(131000, 134000))
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.salary").alias("manager_salary"),
    )
    .orderBy(F.col("manager_salary").desc(), F.col("id").desc())
)
