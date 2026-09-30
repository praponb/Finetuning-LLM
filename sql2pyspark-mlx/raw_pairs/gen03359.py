from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.intern_count") > 6)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ds.l2_count"),
        F.col("ds.intern_count"),
    )
    .orderBy("budget", F.col("intern_count").desc())
)
