from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("dept_name") == "Finance").alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .select(
        F.col("e.id"),
        F.col("e.email"),
        F.col("e.hire_date"),
        F.col("e.manager_id"),
    )
    .orderBy("hire_date")
)
