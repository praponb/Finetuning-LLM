from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.min_salary") > 70000)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("t.avg_salary"),
        F.col("t.min_salary"),
    )
    .orderBy(F.col("min_salary").desc(), F.col("avg_salary").desc())
)
