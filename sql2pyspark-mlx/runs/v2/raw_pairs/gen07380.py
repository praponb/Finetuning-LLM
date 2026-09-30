from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@outlook.com") & F.col("job_title").isin(["Designer", "Analyst"]))
    .groupBy("is_remote", "job_title")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
    .orderBy(F.col("job_title").desc())
)
