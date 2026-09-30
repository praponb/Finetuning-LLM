from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.round((F.col("emp.salary") * 1.07), 2).alias("salary_scaled"),
    )
    .orderBy(F.col("manager_full_name").desc(), F.col("salary_scaled").desc())
)
