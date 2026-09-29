from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("activity_month", F.date_trunc("month", "event_time"))
    .withColumn("week_start", F.date_trunc("week", "event_time"))
    .withColumn("year_start", F.date_trunc("year", "event_time"))
    .rollup("activity_month", "week_start", "year_start")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .orderBy(F.col("activity_month").asc_nulls_first())
)
