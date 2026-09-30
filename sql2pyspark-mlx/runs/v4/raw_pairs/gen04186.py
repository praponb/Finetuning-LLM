from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.lower(F.trim(F.col("m.email"))) == "carol@company.co") | F.col("m.job_title").isNull())
    .select(
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.level").alias("employee_level"),
        F.col("m.salary").alias("manager_salary"),
    )
)
