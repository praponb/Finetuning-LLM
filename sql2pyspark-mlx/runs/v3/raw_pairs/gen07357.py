from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(~F.col("m.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.email").alias("employee_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.email").alias("manager_email"),
        F.split(F.lower(F.col("e.email")), "@").getItem(0).alias("email_user"),
    )
    .orderBy(F.col("manager_salary").desc())
)
