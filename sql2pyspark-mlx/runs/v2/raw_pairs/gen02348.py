from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("email").like("%@startup.io"))
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.stddev_salary") >= 45)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("t.stddev_salary"),
        F.col("t.total_salary"),
    )
)
