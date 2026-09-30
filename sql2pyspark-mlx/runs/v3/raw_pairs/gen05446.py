from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("dept_id").isin([14, 18, 24, 30]))
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("total_count"),
        F.max("hire_date").alias("latest_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("t.total_count"),
        F.col("t.latest_hire_date"),
    )
)
