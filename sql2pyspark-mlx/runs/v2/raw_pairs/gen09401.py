from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("hire_date").alias("min_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.cnt") > 5)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("ds.cnt"),
        F.col("ds.min_hire_date"),
    )
)
