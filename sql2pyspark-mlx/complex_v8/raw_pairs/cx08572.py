from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_id = Window.partitionBy("play_id")
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id", "play_id")

recent = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "party"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 143, "large").when(F.col("bpm") >= 94, "medium").otherwise("small").alias("bpm_tier"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

detail_rows = (
    recent.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.bpm_tier",
        "s.days_since_release_date",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

screened = (
    detail_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.play_listener_id")) & (F.col("p.source").isin("radio", "search", "playlist")) & (~F.col("p.device").isin("speaker", "tv")), "left_anti")
)

annotated = (
    screened
    .filter(F.col("play_ms_played").between(44439, 245877))
    .withColumn("title_ref", F.concat(F.col("title"), F.lit("#"), F.col("play_listener_id").cast("string")))
)

combined_details_play = (
    annotated.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.play_listener_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.moods",
        "s.bpm_tier",
        "s.play_id",
        "s.play_ms_played",
        "s.play_listener_id",
        "s.title_ref",
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("l.interests").alias("listener_interests"),
    )
)

metrics = (
    combined_details_play
    .select(
        "id",
        "title",
        "bpm",
        "moods",
        "bpm_tier",
        "play_id",
        "play_ms_played",
        "play_device",
        "play_source",
        "listener_interests",
        F.round(F.col("bpm") - F.avg("bpm").over(w_play_id), 2).alias("bpm_vs_play_id_avg"),
        (F.col("play_ms_played") - F.lag("play_ms_played").over(w_all_by_bpm)).alias("play_ms_played_change"),
    )
)

result = (
    metrics
    .select(
        "id",
        "title",
        "bpm",
        "moods",
        "bpm_tier",
        "play_id",
        "play_ms_played",
        "play_device",
        "play_source",
        "listener_interests",
        "bpm_vs_play_id_avg",
        "play_ms_played_change",
    )
)
