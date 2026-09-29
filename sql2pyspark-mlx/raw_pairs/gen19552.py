from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("level").isNotNull())
    .groupBy("dept_id")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .filter(F.col("dt.max_salary") > 126000)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("dt.max_salary"),
    )
)
