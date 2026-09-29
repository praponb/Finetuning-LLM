from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("dept_id").isin([11, 25]))
    .groupBy("dept_id")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .select(
        F.col("d.budget"),
        F.col("d.location"),
        F.col("D.avg_salary"),
        F.col("D.total_salary"),
    )
    .orderBy(F.col("avg_salary").desc())
)
