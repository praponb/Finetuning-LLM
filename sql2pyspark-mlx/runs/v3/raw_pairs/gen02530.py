from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.length("email") > 14)
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Intern", 1).otherwise(0)).alias("intern_count"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ds.intern_count"),
        F.col("ds.pct_analyst"),
    )
    .orderBy("location")
)
