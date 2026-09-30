from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.is_remote"))
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.last_day(F.col("m.hire_date")).alias("hire_date_month_end"),
    )
    .orderBy("employee_email")
)
