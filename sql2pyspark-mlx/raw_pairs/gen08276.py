from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("id", "event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("event_type").isin(["search", "add_to_cart"]))
        .select("id", "event_type")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("session_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("page") == "/search").alias("search_events"),
        F.avg(F.when(F.col("event_type") == "page_view", F.col("duration_sec"))).alias("page_view_avg_duration_sec"),
        F.percentile_approx("duration_sec", [0.25, 0.5, 0.75]).alias("duration_sec_quartiles"),
    )
)
