from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary")).otherwise(0)).alias("analyst_salary"),
        F.count(F.when(F.col("level") == "L4", 1)).alias("l4_count"),
    )
)

result = (
    df_departments.alias("departments")
    .join(department_totals.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.l4_count") > 8)
    .select(
        F.col("departments.dept_name"),
        F.col("departments.location"),
        F.col("t.analyst_salary"),
        F.col("t.l4_count"),
    )
    .orderBy("location")
)
