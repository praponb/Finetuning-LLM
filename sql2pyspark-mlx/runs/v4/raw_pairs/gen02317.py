from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-12-22")))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.trim("event_type").alias("clean_event_type"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

result = (
    base
    .groupBy("event_type")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
    )
)
