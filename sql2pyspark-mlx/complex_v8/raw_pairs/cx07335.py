from pyspark.sql import functions as F

prepared_plays = (
    df_plays
    .filter((F.col("source").isin("search", "album")) & (F.col("ms_played").between(52009, 324437)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
    )
)

with_parent = (
    prepared_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.device_clean",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.country").alias("listener_country"),
        F.col("l.username").alias("listener_username"),
        F.col("t.title").alias("track_title"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    with_parent
    .groupBy("track_id")
    .pivot("listener_country", ["JP", "KR", "UK", "TH"])
    .agg(F.max("track_duration_sec").alias("max_v"), F.sum("track_duration_sec").alias("sum_v"))
    .orderBy("track_id")
)
