from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .select(
        F.col("d.budget"),
        F.col("d.location"),
        F.col("dt.l3_count"),
        F.col("dt.intern_count"),
    )
)
