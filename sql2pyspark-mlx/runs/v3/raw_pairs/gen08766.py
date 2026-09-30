from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("manager_id") == 3)
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("total_count"),
        F.min("hire_date").alias("min_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("ds.total_count"),
        F.col("ds.min_hire_date"),
    )
    .orderBy("location", "id")
    .limit(5)
)
