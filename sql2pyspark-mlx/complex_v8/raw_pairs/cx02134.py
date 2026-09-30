from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id = Window.partitionBy("artist_id")

recent_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) | (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

ranked = (
    recent_tracks
    .select(
        "id",
        "title",
        "duration_sec",
        "bpm",
        F.dense_rank().over(w_artist_id_by_duration_sec).alias("duration_sec_dense_rank"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
    )
)

derived = (
    ranked
    .filter(F.col("duration_sec").between(144, 392))
    .select(
        "*",
        (F.floor(F.col("duration_sec_dense_rank") / 2) * 2).alias("duration_sec_dense_rank_bucket"),
    )
)

with_activity = (
    derived.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left_semi")
    .filter(F.col("duration_sec_share_of_artist_id") > 0.3)
)

with_children_plays = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.duration_sec",
        "s.duration_sec_dense_rank",
        "s.duration_sec_share_of_artist_id",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

filtered = (
    with_children_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id").isNotNull()), "left_semi")
)

with_parent_listener = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.play_listener_id"), "left")
    .select(
        "s.id",
        "s.duration_sec_dense_rank",
        "s.play_id",
        "s.play_listener_id",
        "s.play_ms_played",
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    with_parent_listener
    .select(
        "id",
        "play_id",
        "play_ms_played",
        "listener_birth_year",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["play_ms_played", "listener_birth_year"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "play_id", "field")
)
