from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.length("full_name") > 11)
    .groupBy("dept_id")
    .agg(F.avg("salary").alias("avg_salary"))
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "inner")
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("department_stats.avg_salary"),
    )
)
