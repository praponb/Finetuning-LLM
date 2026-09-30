from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter((F.col("source") != "album") | (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

labeled = (
    filtered_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

enriched_listener = (
    labeled.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.signup_date").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.source",
        "s.days_since_played_at",
        "s.played_at_label",
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

with_details = (
    enriched_listener.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.size(F.col("l.interests")) >= 2), "inner")
    .filter(~F.col("l.country").isin("JP", "UK"))
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.source",
        "s.days_since_played_at",
        "s.played_at_label",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.country").alias("listener_country"),
    )
)

result = (
    with_details
    .groupBy("listener_plan")
    .pivot("listener_country", ["KR", "UK", "BR", "JP"])
    .agg(F.count("days_since_played_at"))
)
