from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.avg_salary") >= 72)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("dt.cnt"),
        F.col("dt.avg_salary"),
    )
    .orderBy(F.col("id").desc(), F.col("cnt").desc())
)
