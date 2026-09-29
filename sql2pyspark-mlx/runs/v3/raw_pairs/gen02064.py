from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.min("hire_date").alias("min_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("ds.last_hire_date"),
        F.col("ds.min_hire_date"),
    )
)
