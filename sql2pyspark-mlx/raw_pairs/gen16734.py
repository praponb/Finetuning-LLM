from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("t.salary_range"),
    )
    .orderBy(F.col("location").desc(), "id")
    .limit(3)
)
