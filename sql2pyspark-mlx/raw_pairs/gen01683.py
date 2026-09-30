from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.year("hire_date") == 2025)
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_managers"))
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "inner")
    .filter(F.col("department_totals.unique_managers") > 9)
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("departments.location"),
        F.col("department_totals.unique_managers"),
    )
    .orderBy(F.col("budget").desc())
)
