from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("salary").alias("min_salary"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("ds.min_salary"), F.col("ds.avg_salary"))
    .orderBy("avg_salary")
)
