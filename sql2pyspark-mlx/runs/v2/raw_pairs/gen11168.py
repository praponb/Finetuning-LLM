from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0))) / F.count("*")), 1).alias("pct_director"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("D"), F.col("d.id") == F.col("D.dept_id"), "left")
    .filter(F.col("D.pct_director") > 53)
    .select(F.col("d.location"), F.col("D.pct_director"))
    .orderBy(F.col("pct_director").desc())
)
