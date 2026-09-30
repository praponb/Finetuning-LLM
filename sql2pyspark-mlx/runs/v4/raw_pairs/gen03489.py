from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-09-28").cast("date"))
    .select(
        "*",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
        F.initcap(F.trim("page")).alias("page_title"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

result = (
    filtered
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .cube("session_id", "week_start")
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max("event_time").alias("ended_at"),
        F.grouping("session_id").alias("grouping_level"),
    )
    .filter(F.col("span_minutes") >= 0)
    .orderBy(F.col("session_id").desc())
)
