from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("level").isin(["L2", "L1"]))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("department_totals"), F.col("departments.id") == F.col("department_totals.dept_id"), "left")
    .filter(F.col("department_totals.analyst_count") >= 79)
    .select(F.col("departments.dept_name"), F.col("department_totals.analyst_count"))
    .orderBy("dept_name", F.col("analyst_count").desc())
)
