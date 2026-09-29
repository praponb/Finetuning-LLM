from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.id"), F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.round(((100 * F.sum(F.when(F.col("e.job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
    )
    .orderBy(
        F.col("pct_analyst").desc(),
        F.col("employee_count").desc(),
        "id",
        "location",
    )
    .limit(20)
)
