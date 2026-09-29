from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("level") == "L2")
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("DepartmentTotals"), F.col("departments.id") == F.col("DepartmentTotals.dept_id"), "inner")
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("departments.budget"),
        F.col("DepartmentTotals.total_count"),
        F.col("DepartmentTotals.cnt"),
    )
    .orderBy(F.col("cnt").desc())
)
