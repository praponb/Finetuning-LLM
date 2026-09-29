from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .filter(F.col("dt.stddev_salary") >= 70)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("dt.first_hire_date"),
        F.col("dt.stddev_salary"),
    )
    .orderBy(F.col("id").desc())
)
