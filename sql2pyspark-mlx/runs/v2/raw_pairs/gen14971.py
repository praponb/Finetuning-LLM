from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("dept_name") != "Legal").alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_anti")
    .select(
        F.col("e.id"),
        F.col("e.is_remote"),
        F.col("e.full_name"),
        F.col("e.hire_date"),
    )
)
