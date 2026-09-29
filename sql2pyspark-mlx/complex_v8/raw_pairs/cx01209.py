from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source = Window.partitionBy("source")
w_source_len_track_id_by_track = Window.partitionBy("source_len", "track_id").orderBy(F.col("track_duration_sec").desc())
w_source_len_track_id_by_track_2 = Window.partitionBy("source_len", "track_id").orderBy(F.col("track_duration_sec").desc(), "id")
w_source_len_track_id_by_track_3 = Window.partitionBy("source_len", "track_id").orderBy(F.col("track_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_track_duration_sec_vs = Window.orderBy("track_duration_sec_vs_source_avg", "id", "play_id")
w_all_by_play_played_at_ties = Window.orderBy(F.col("play_played_at").desc())
w_track_id_play_source = Window.partitionBy("track_id", "play_source")

cleaned = (
    df_plays
    .filter((F.col("track_id") <= 4) | (F.col("ms_played") > 62294))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
    )
)

enriched_track = (
    cleaned.alias("s")
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
        "s.source_len",
        "s.played_at_date",
        "s.source_prefix",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.title").alias("track_title"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

ranked = (
    enriched_track
    .select(
        "*",
        F.size(F.collect_set("listener_id").over(w_source)).alias("distinct_listener_id_in_source"),
        F.dense_rank().over(w_source_len_track_id_by_track).alias("track_duration_sec_dense_rank"),
        (F.col("ms_played") - F.lag("ms_played").over(w_source_len_track_id_by_track_2)).alias("ms_played_change"),
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_source), 2).alias("track_duration_sec_vs_source_avg"),
        F.last("played_at").over(w_source_len_track_id_by_track_3).alias("last_played_at"),
    )
)

with_children = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source_len",
        "s.played_at_date",
        "s.source_prefix",
        "s.track_title",
        "s.track_duration_sec",
        "s.distinct_listener_id_in_source",
        "s.ms_played_change",
        "s.track_duration_sec_vs_source_avg",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

sequenced = (
    with_children
    .select(
        "*",
        F.lag("play_played_at", 2).over(w_all_by_track_duration_sec_vs).alias("prev2_play_played_at"),
        F.dense_rank().over(w_all_by_play_played_at_ties).alias("play_played_at_dense_rank"),
        F.round(F.col("play_ms_played") - F.avg("play_ms_played").over(w_track_id_play_source), 2).alias("play_ms_played_vs_track_id_play_source_avg"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "play_id",
        "play_played_at_dense_rank",
        "source_len",
        "play_ms_played",
        "track_duration_sec",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["play_played_at_dense_rank", "source_len", "play_ms_played", "track_duration_sec"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
