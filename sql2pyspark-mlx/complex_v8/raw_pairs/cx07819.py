from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_listener_id = Window.partitionBy("is_skipped", "listener_id")

valid_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
        F.abs(F.col("ms_played") - 129336).alias("ms_played_distance"),
    )
)

metrics = (
    valid_plays
    .select(
        "*",
        F.size(F.collect_set("track_id").over(w_is_skipped_listener_id)).alias("distinct_track_id_in_is_skipped_listener_id"),
        F.size(F.collect_set("device").over(w_is_skipped_listener_id)).alias("distinct_device_in_is_skipped_listener_id"),
        F.count(F.lit(1)).over(w_is_skipped_listener_id).alias("rows_in_is_skipped_listener_id"),
    )
)

joined = (
    metrics.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.played_at_plus_1d",
        "s.ms_played_distance",
        "s.distinct_track_id_in_is_skipped_listener_id",
        "s.distinct_device_in_is_skipped_listener_id",
        "s.rows_in_is_skipped_listener_id",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

result = (
    joined
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at_plus_1d",
        "ms_played_distance",
        "distinct_track_id_in_is_skipped_listener_id",
        "distinct_device_in_is_skipped_listener_id",
        "track_release_date",
        "track_bpm",
        "track_is_explicit",
        F.when(F.col("ms_played_distance") >= 300000, "large").when(F.col("ms_played_distance") >= 80000, "medium").otherwise("small").alias("ms_played_distance_tier"),
    )
    .orderBy("track_bpm", F.col("id").desc())
)
