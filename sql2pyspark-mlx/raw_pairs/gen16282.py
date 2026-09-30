from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 122000, "PREMIUM").otherwise("STANDARD").alias("salary_tier"),
    )
    .agg(
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
        F.sum(F.when(F.col("job_title") == "Analyst", F.col("salary"))).alias("analyst_total_salary"),
        F.stddev_samp("salary").alias("salary_stddev"),
    )
    .filter(F.col("l2_salary") > 0)
    .orderBy(F.col("analyst_total_salary").desc_nulls_first())
)
