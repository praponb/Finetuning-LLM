from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.count_if(F.col("location") == "Berlin").alias("berlin_events"),
        F.max("budget").alias("max_budget"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
    )
    .orderBy(F.col("budget_stddev").desc_nulls_last())
)
