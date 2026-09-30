from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter((F.col("d.location") == "Berlin") | ~F.col("e.full_name").like("Dav%"))
    .select(F.col("e.job_title"), F.col("d.dept_name"))
    .orderBy(F.col("dept_name").desc())
)
