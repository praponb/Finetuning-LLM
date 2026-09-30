from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sum(F.when(F.col("level") == "L1", 1).otherwise(0)).alias("l1_count"))
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("DepartmentTotals"), F.col("departments.id") == F.col("DepartmentTotals.dept_id"), "inner")
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("DepartmentTotals.l1_count"),
    )
    .orderBy("l1_count", "id")
    .limit(5)
)
