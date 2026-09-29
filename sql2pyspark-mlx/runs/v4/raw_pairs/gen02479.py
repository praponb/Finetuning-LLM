from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(
        F.when(F.col("salary") >= 161000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.avg("salary").alias("avg_salary"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("level") == "L5", 1)), F.lit(0)), 2).alias("salary_per_l5"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
    )
    .orderBy(F.col("avg_salary").desc_nulls_last())
)
