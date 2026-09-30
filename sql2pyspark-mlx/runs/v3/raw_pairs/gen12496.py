from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.manager_id").isin([2, 19, 27, 30]))
    .select(
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.email").alias("employee_email"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy("manager_full_name")
)
