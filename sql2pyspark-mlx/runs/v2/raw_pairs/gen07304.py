from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("full_name").like("b%"))
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.salary_range") > 71)
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("ds.salary_range"),
    )
    .orderBy(F.col("id").desc())
)
