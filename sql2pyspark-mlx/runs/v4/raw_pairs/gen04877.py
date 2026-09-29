from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("m.email").alias("manager_email"),
        F.date_format(F.col("m.hire_date"), "yyyy-MM").alias("hire_date_label"),
    )
    .orderBy("employee_email")
)
