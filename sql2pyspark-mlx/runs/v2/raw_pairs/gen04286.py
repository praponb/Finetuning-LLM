from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("e.salary") < 147000) & F.col("e.manager_id").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy("employee_hire_date")
)
