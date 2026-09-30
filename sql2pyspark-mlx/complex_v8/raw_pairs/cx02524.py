from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("play_source_2").orderBy(F.col("play_played_at_quarter").desc(), F.col("id").desc(), F.col("play_id").desc())

base = (
    df_tracks
    .filter((F.col("bpm").between(85, 166)) & (F.size(F.col("moods")) >= 2))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

line_level_plays = (
    base.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.moods",
        "s.title_len",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

enriched = (
    line_level_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("p.listener_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.moods",
        "s.play_id",
        "s.play_ms_played",
        "s.play_played_at",
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.source").alias("play_source_2"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("l.country").alias("listener_country"),
    )
)

derived = (
    enriched
    .select(
        "*",
        F.abs(F.col("bpm") - 108).alias("bpm_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("play_source_2")), " ", "_")).alias("play_source_2_slug"),
        (F.col("play_ms_played") + F.col("bpm")).alias("play_ms_played_plus_bpm"),
    )
)

annotated = (
    derived
    .filter(F.array_contains(F.col("moods"), "sad"))
    .select(
        "id",
        "bpm",
        "moods",
        "play_id",
        "play_ms_played",
        "play_played_at",
        "play_source_2",
        "listener_country",
        "play_ms_played_plus_bpm",
        F.quarter(F.col("play_played_at")).alias("play_played_at_quarter"),
        (F.col("play_played_at") + F.expr("INTERVAL 7 DAYS")).alias("play_played_at_plus_7d"),
    )
)

deduped = (
    annotated
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .groupBy("play_played_at")
    .pivot("listener_country", ["TH", "JP", "KR"])
    .agg(F.sum("bpm"))
)
