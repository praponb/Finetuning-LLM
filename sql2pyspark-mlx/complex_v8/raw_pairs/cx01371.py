from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.when(F.col("ms_played") >= 221269, "A").when(F.col("ms_played") >= 97825, "B").otherwise("C").alias("ms_played_tier"),
    )
)

with_details = (
    prepared_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
        F.col("t.moods").alias("track_moods"),
    )
)

labeled = (
    with_details
    .withColumn("listener_signup_date_label", F.date_format(F.col("listener_signup_date"), "yyyy-MM"))
)

result = (
    labeled
    .select(
        "id",
        "played_at",
        "is_skipped",
        "source",
        "listener_signup_date",
        "listener_country",
        "listener_plan",
        "track_moods",
        "listener_signup_date_label",
        F.hour(F.col("played_at")).alias("played_at_hour"),
    )
)
