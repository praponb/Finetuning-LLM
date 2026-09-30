from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_ms_played_ties = Window.partitionBy("track_id").orderBy("ms_played")
w_track_id = Window.partitionBy("track_id")

base_plays = (
    df_plays
    .filter(F.col("ms_played") > 56858)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 192595).alias("ms_played_distance"),
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
        F.concat(F.col("device"), F.lit("#"), F.col("ms_played").cast("string")).alias("device_ref"),
    )
)

with_windows = (
    base_plays
    .withColumn("ms_played_dense_rank", F.dense_rank().over(w_track_id_by_ms_played_ties))
    .withColumn("distinct_listener_id_in_track_id", F.size(F.collect_set("listener_id").over(w_track_id)))
)

with_details = (
    with_windows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select("s.*", F.col("t.title").alias("track_title"))
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

with_counts = (
    with_details.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

first_rows = (
    with_counts
    .filter(F.col("ms_played_dense_rank") <= 3)
    .drop("ms_played_dense_rank")
)

result = (
    first_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "ms_played_distance",
        "device_source_label",
        "device_ref",
        "distinct_listener_id_in_track_id",
        "track_title",
        "min_play_ms_played",
        "play_is_skipped_count",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
    .orderBy("source_or_unknown")
)
