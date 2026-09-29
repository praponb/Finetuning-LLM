from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(F.max("hire_date").alias("last_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ds.last_hire_date"),
    )
    .orderBy("last_hire_date", "budget")
)
