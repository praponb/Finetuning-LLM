from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_track_is_explici = Window.partitionBy("listener_id", "track_is_explicit")
w_track_is_explicit_by_played = Window.partitionBy("track_is_explicit").orderBy(F.col("played_at").desc_nulls_first(), "id")
w_device_by_played_at = Window.partitionBy("device").orderBy(F.col("played_at").desc(), "id")
w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_track_is_explicit_by_played_2 = Window.partitionBy("track_is_explicit").orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_listener_id = Window.partitionBy("listener_id")
w_track_is_explicit_by_played_3 = Window.partitionBy("track_is_explicit").orderBy(F.col("played_at").desc_nulls_first())

base = (
    df_plays
    .filter(F.col("device") != "speaker")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

joined_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

sequenced = (
    joined_track
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        "track_is_explicit",
        "listener_signup_date",
        F.min("ms_played").over(w_listener_id_track_is_explici).alias("listener_id_track_is_explicit_min_ms_played"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id_track_is_explici), F.lit(0)), 4).alias("ms_played_share_of_listener_id_track_is_explicit"),
        F.dense_rank().over(w_track_is_explicit_by_played).alias("played_at_dense_rank"),
        F.rank().over(w_device_by_played_at).alias("played_at_rank"),
        F.round(F.cume_dist().over(w_track_id_by_ms_played), 3).alias("ms_played_cume_dist"),
        F.max("ms_played").over(w_track_is_explicit_by_played_2).alias("ms_played_max_3_prior"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id"),
        F.dense_rank().over(w_track_is_explicit_by_played_3).alias("played_at_dense_rank_2"),
    )
)

result = (
    sequenced
    .filter(F.col("ms_played_max_3_prior") >= 244000)
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        "track_is_explicit",
        "listener_signup_date",
        "listener_id_track_is_explicit_min_ms_played",
        "ms_played_share_of_listener_id_track_is_explicit",
        "played_at_rank",
        "ms_played_cume_dist",
        "ms_played_max_3_prior",
        "played_at_dense_rank_2",
        F.coalesce(F.col("ms_played_cume_dist"), F.lit(0.0)).alias("ms_played_cume_dist_filled"),
    )
    .orderBy(F.col("ms_played_max_3_prior").desc())
)
