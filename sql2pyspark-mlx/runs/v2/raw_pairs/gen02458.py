from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("dt.avg_salary"),
        F.col("dt.max_salary"),
    )
    .orderBy(F.col("avg_salary").desc())
)
