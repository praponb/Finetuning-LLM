from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("level").isin(["L3", "L4", "L1"]))
    .groupBy("dept_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.avg_salary") >= 38)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.location"),
        F.col("t.avg_salary"),
    )
)
