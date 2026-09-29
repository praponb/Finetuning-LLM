from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("email").like("%@outlook.com"))
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.max_salary") >= 97000)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("dt.unique_levels"),
        F.col("dt.max_salary"),
    )
)
