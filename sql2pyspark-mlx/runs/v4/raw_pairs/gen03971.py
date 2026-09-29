from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .groupBy(F.col("d.location"), F.col("d.dept_name"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("e.job_title") == "Engineer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineer"),
    )
    .filter(F.col("pct_engineer") >= 47)
    .orderBy(F.col("pct_engineer").desc(), "location", "dept_name")
    .limit(10)
)
