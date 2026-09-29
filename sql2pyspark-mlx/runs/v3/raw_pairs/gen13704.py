from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.count("*").alias("employee_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ds.avg_salary"),
        F.col("ds.employee_count"),
    )
)
