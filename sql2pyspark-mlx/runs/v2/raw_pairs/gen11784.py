from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("full_name").like("%Kim%"))
    .groupBy("dept_id")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.max_salary") > 95000)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ds.max_salary"),
    )
)
