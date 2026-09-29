from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("dept_name") != "Legal").alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.dept_id").isin([9, 25]))
    .select(
        F.col("e.id"),
        F.col("e.hire_date"),
        F.col("e.job_title"),
        F.col("e.dept_id"),
    )
    .orderBy("hire_date")
)
