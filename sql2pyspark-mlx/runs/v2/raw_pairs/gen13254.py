from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.countDistinct("salary").alias("unique_depts"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.unique_depts") >= 4)
    .select(F.col("d.budget"), F.col("ds.salary_range"), F.col("ds.unique_depts"))
)
