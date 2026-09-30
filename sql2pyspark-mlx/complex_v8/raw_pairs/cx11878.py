from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_prior_6 = Window.orderBy("ms_played", "id").rowsBetween(-6, -1)
w_track_id = Window.partitionBy("track_id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")

recent = (
    df_plays
    .filter((F.col("track_id").isNotNull()) | (F.col("listener_id").isNotNull()))
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

with_trends = (
    recent
    .select(
        "*",
        F.max("ms_played").over(w_all_by_ms_played_prior_6).alias("ms_played_max_6_prior"),
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id"),
        F.row_number().over(w_all_by_ms_played).alias("ms_played_seq"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        "s.ms_played_max_6_prior",
        "s.ms_played_seq",
        F.col("t.is_explicit").alias("track_is_explicit"),
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.sum("ms_played").alias("total_play_ms_played"),
        F.min("played_at").alias("first_played_at"),
        F.min("played_at").alias("first_played_at_2"),
    )
)

profiled = (
    joined.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.total_play_ms_played",
        "p.first_played_at",
        "p.first_played_at_2",
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
    )
)

with_stats = (
    profiled.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.min_play_ms_played", "p.last_played_at")
)

top_per_group = with_stats.filter(F.col("ms_played_seq") <= 2)

result = (
    top_per_group
    .filter(F.col("ms_played_seq") < 8)
    .select(
        "id",
        "track_id",
        "played_at",
        "device",
        "ms_played_max_6_prior",
        "ms_played_seq",
        "track_is_explicit",
        "artist_name",
        "artist_country",
        "total_play_ms_played",
        "first_played_at_2",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        (F.floor(F.col("ms_played_seq") / 2) * 2).alias("ms_played_seq_bucket"),
    )
    .orderBy(F.col("track_id").desc(), "artist_country")
)
