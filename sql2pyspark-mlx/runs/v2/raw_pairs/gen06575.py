from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .groupBy("dept_id")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.stddev_salary") >= 70)
    .select(F.col("d.dept_name"), F.col("t.stddev_salary"))
    .orderBy("stddev_salary")
)
