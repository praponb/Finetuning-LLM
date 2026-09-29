from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country_track_id = Window.partitionBy("artist_country", "track_id")
w_played_at_month_by_ms_played = Window.partitionBy(F.month("played_at")).orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_month_by_ms_played_2 = Window.partitionBy(F.month("played_at")).orderBy("ms_played", "id")
w_dedup = Window.partitionBy("track_artist_id").orderBy("played_at", "id")

cleaned = (
    df_plays
    .filter(F.col("played_at") < F.lit("2025-02-24 09:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
)

without_matches = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left_semi")
)

joined = (
    without_matches.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_prefix",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.country").alias("listener_country"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

filtered = (
    joined.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")) & (F.col("p.device") != "desktop"), "left_semi")
)

with_parent = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.*",
        F.col("t.title").alias("track_title"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

windowed = (
    with_parent
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "listener_country",
        "track_bpm",
        "track_title",
        "track_artist_id",
        "track_duration_sec",
        "artist_name",
        "artist_country",
        F.round(F.avg("track_bpm").over(w_artist_country_track_id), 2).alias("artist_country_track_id_avg_track_bpm"),
        F.count(F.lit(1)).over(w_artist_country_track_id).alias("rows_in_artist_country_track_id"),
        F.first("track_bpm").over(w_played_at_month_by_ms_played).alias("first_track_bpm"),
        F.lag("track_bpm").over(w_played_at_month_by_ms_played_2).alias("prev_track_bpm"),
        F.min("track_bpm").over(w_artist_country_track_id).alias("artist_country_track_id_min_track_bpm"),
        F.max("track_duration_sec").over(w_artist_country_track_id).alias("artist_country_track_id_max_track_duration_sec"),
    )
)

latest = (
    windowed
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .groupingSets(
        [["listener_country", "listener_id", "track_id"], ["listener_country"], ["listener_id"], []],
        "listener_country", "listener_id", "track_id",
    )
    .agg(
        F.grouping("listener_country").alias("is_total_listener_country"),
        F.count(F.when(F.col("device").isin("speaker", "desktop"), 1)).alias("n_device_in_speaker_desktop"),
        F.max("played_at").alias("last_played_at"),
        F.max("rows_in_artist_country_track_id").alias("max_rows_in_artist_country_track_id"),
    )
)
