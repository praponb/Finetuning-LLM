from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_track_artist_id_by_played_at = Window.partitionBy("track_artist_id").orderBy(F.col("played_at_hour").desc(), "id")

valid_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

qualified = (
    valid_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") >= 92277) & (F.col("p.track_id").isNotNull()), "left_semi")
    .filter(F.col("played_at_hour") < F.lit("2025-02-05 09:00:00").cast("timestamp"))
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    qualified.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

joined = (
    summarized_rows.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.release_date") >= F.lit("2024-10-23").cast("date")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.played_at_hour",
        "s.device_source_label",
        "s.source_or_unknown",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.moods").alias("track_moods"),
    )
)

sequenced = (
    joined
    .select(
        "id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "device_source_label",
        "source_or_unknown",
        "track_artist_id",
        "track_moods",
        F.size(F.collect_set("source_or_unknown").over(w_listener_id)).alias("distinct_source_or_unknown_in_listener_id"),
        F.row_number().over(w_track_artist_id_by_played_at).alias("seq_no"),
        F.round(F.avg("ms_played").over(w_listener_id), 2).alias("listener_id_avg_ms_played"),
    )
)

result = (
    sequenced
    .select("track_artist_id")
    .distinct()
    .orderBy(F.col("track_artist_id").asc())
)
