from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.unique_depts") > 7)
    .select(
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ds.unique_depts"),
        F.col("ds.last_hire_date"),
    )
)
