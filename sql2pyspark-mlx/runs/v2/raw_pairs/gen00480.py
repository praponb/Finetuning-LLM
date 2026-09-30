from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        F.col("e.salary"),
        F.col("e.dept_id"),
        F.col("e.is_remote"),
        F.col("d.dept_name"),
        F.lower(F.col("e.email")).alias("email_lower"),
    )
)
