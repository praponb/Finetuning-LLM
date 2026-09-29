from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L3", 1).otherwise(0)).alias("l3_count"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "left")
    .filter(F.col("department_totals.total_salary") > 348000)
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("department_totals.l3_count"),
        F.col("department_totals.total_salary"),
    )
)
