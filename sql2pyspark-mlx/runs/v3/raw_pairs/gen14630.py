from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.full_name").like("Da%") | (F.upper(F.trim(F.col("e.email"))) == "EMMA@OUTLOOK.COM"))
    .select(
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.level").alias("manager_level"),
        F.lower(F.col("e.email")).alias("email_lower"),
    )
    .orderBy("employee_hire_date")
)
