from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.count("*").alias("row_count"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
    )
    .orderBy(F.col("salary_p95").desc_nulls_last())
)
