from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.avg_salary") >= 148000)
    .select(F.col("d.location"), F.col("ds.avg_salary"), F.col("ds.max_salary"))
    .orderBy(F.col("avg_salary").desc())
)
