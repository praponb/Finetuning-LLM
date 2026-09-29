from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter((F.col("e.hire_date") <= "2023-04-01") & F.col("e.level").isin(["L1", "L4"]))
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.email").alias("manager_email"),
    )
    .orderBy("manager_email")
)
