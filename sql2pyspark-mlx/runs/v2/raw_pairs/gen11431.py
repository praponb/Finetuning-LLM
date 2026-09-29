from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
        F.sum(F.when(F.col("level") == "L4", 1).otherwise(0)).alias("l4_count"),
    )
    .orderBy(F.col("dept_id").desc(), "pct_analyst")
)
