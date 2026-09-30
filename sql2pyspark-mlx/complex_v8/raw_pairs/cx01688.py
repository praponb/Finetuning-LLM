from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_country").orderBy(F.col("row_count").desc(), F.col("played_at_ym").desc(), F.col("listener_id").desc(), F.col("device").desc())

filtered_plays = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) | (F.col("source") == "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.dayofweek(F.col("played_at")).alias("played_at_dayofweek"),
    )
)

enriched_track = (
    filtered_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select("s.*", F.col("t.moods").alias("track_moods"))
)

stats = (
    enriched_track
    .filter(F.array_contains(F.col("track_moods"), "party"))
    .groupBy(
        F.date_format("played_at", "yyyy-MM").alias("played_at_ym"),
        "listener_id",
        "device",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("source").isin("playlist", "radio", "album"), 1)).alias("n_source_in_playlist_radio_alb"),
    )
    .filter(F.col("row_count") >= 2)
)

with_details_listener = (
    stats.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.col("l.country").alias("listener_country"),
        F.col("l.username").alias("listener_username"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

latest = (
    with_details_listener
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "played_at_ym",
        "row_count",
        "n_source_in_playlist_radio_alb",
        "listener_country",
        "listener_username",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
    )
    .orderBy(F.col("device_or_unknown").desc_nulls_first())
)
