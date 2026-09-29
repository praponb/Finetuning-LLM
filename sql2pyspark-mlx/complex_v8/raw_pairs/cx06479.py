from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_ms_played = Window.partitionBy("device").orderBy("ms_played", "id")
w_device_by_ms_played_full = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_plays
    .filter(~F.col("device").isin("speaker", "phone"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
    )
)

windowed = (
    prepared
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.row_number().over(w_device_by_ms_played).alias("ms_played_seq"),
        F.nth_value("played_at", 2).over(w_device_by_ms_played_full).alias("second_played_at"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_device_by_ms_played))) / 60, 1).alias("minutes_since_prev_played_at"),
    )
)

enriched_track = (
    windowed.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.ms_played_seq",
        "s.second_played_at",
        "s.minutes_since_prev_played_at",
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

enriched_artist = (
    enriched_track.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.track_artist_id"), "inner")
    .filter(F.col("a.country").isNotNull())
    .select(
        "s.id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.ms_played_seq",
        "s.second_played_at",
        "s.minutes_since_prev_played_at",
        "s.track_artist_id",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
    )
)

filtered = (
    enriched_artist.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.is_skipped")), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "track_id",
        "ms_played",
        "ms_played_seq",
        "artist_is_verified",
        F.greatest(F.col("ms_played"), F.col("ms_played_seq")).alias("greatest_ms_played_ms_played_seq"),
        F.when(F.col("ms_played") >= 299017, "large").when(F.col("ms_played") >= 168782, "medium").otherwise("small").alias("ms_played_tier"),
    )
    .orderBy(F.col("ms_played_tier").desc())
)
