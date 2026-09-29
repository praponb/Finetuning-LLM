from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("t.stddev_salary"), F.col("t.l1_count"))
    .orderBy(F.col("dept_name").desc())
)
