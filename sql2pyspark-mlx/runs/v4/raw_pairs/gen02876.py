from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.email").alias("employee_email"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.round((F.col("m.salary") * 1.1), 2).alias("salary_scaled"),
    )
)
