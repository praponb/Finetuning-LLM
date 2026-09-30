from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("email").isNotNull())
    .select("*", F.substring("full_name", 1, 3).alias("full_name_prefix"))
)

result = (
    filtered
    .groupBy(F.date_trunc("year", "hire_date").alias("year_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("salary", 0.95).alias("salary_p95"),
        F.stddev_samp("salary").alias("salary_stddev"),
        F.count("*").alias("event_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .orderBy(F.col("event_count").desc())
)
