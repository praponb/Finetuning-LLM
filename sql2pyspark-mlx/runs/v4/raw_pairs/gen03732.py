from pyspark.sql import functions as F

remaining_keys = (
    df_web_events
    .select("event_type", "page")
    .intersectAll(
        df_web_events
        .filter(F.col("page") == "/search")
        .select("event_type", "page")
    )
)

verified = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"), "event_type")
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.max("event_time").alias("ended_at"),
        F.count_if(F.col("event_type") == "search").alias("search_events"),
    )
    .orderBy(F.col("week_start").desc())
)
