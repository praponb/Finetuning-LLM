from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.min("hire_date").alias("min_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("ds.total_salary"),
        F.col("ds.min_hire_date"),
    )
    .orderBy(F.col("id").desc(), F.col("total_salary").desc())
)
