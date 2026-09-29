from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("t.latest_hire_date"),
    )
)
