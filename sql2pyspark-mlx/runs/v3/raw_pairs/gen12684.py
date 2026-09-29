from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("first_hire_date"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("ds.first_hire_date"),
        F.col("ds.pct_analyst"),
    )
    .orderBy(F.col("pct_analyst").desc())
)
