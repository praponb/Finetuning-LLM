from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.dept_id").isin([17, 25, 35]))
    .groupBy(F.col("d.location"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("e.job_title") == "Engineer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineer"),
        F.avg(F.col("e.salary")).alias("avg_salary"),
    )
)
