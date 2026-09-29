from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .filter(F.col("d.budget") >= 1081000)
    .groupBy(F.col("d.location"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("e.job_title") == "Intern", 1).otherwise(0))) / F.count("*")), 1).alias("pct_intern"),
    )
    .filter(F.col("pct_intern") > 28)
)
