from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("ds.pct_designer"),
        F.col("ds.total_count"),
    )
)
