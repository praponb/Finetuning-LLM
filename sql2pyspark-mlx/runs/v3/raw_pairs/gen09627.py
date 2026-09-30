from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(
        F.col("e.is_remote"),
        F.col("e.dept_id"),
        F.col("d.dept_name"),
        F.date_format(F.col("e.hire_date"), "yyyy-MM").alias("hire_date_label"),
    )
    .orderBy(F.col("hire_date_label").desc(), F.col("dept_id").desc())
)
