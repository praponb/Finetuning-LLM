from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_listener_id_play_device = Window.partitionBy("play_listener_id", "play_device").orderBy(F.col("play_ms_played").desc(), "id", "play_id")

cleaned_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("S%")) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
    )
)

line_level = (
    cleaned_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device") == "tv"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
    )
)

ranked = (
    line_level
    .select(
        "*",
        F.ntile(4).over(w_play_listener_id_play_device).alias("play_ms_played_quartile"),
        F.lag("duration_sec").over(w_play_listener_id_play_device).alias("prev_duration_sec"),
    )
)

play_device_stats = (
    ranked
    .groupBy("play_device")
    .agg(F.max("bpm").alias("max_bpm"))
)

vs_group = (
    ranked.alias("s")
    .join(play_device_stats.alias("g"), F.col("g.play_device") == F.col("s.play_device"), "inner")
    .filter(F.col("s.bpm") == F.col("g.max_bpm"))
    .select(
        "s.*",
        "g.max_bpm",
        F.round(F.col("s.bpm") - F.col("g.max_bpm"), 2).alias("bpm_minus_max"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "play_id",
        "play_listener_id",
        "play_played_at",
        "play_ms_played",
        "play_device",
        "prev_duration_sec",
        "bpm_minus_max",
    )
)
