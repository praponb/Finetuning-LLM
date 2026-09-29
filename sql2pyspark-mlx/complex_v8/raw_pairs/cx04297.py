from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("track_id").orderBy(F.col("last_played_at").desc(), F.col("id").desc())
w_device_listener_id = Window.partitionBy("device", "listener_id")
w_source_listener_id_by_ms_pla = Window.partitionBy("source", "listener_id").orderBy("ms_played", "id")

valid_plays = df_plays.filter(F.col("listener_id") >= 4)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_counts = (
    valid_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.last_played_at")
)

joined = (
    with_counts.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.*",
        F.col("t.moods").alias("track_moods"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

combined_details_track = (
    joined.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.is_explicit")), "inner")
    .select(
        "s.*",
        F.col("t.bpm").alias("track_bpm_2"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
    )
)

latest = (
    combined_details_track
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_trends = (
    latest
    .select(
        "*",
        F.size(F.collect_set("track_id").over(w_device_listener_id)).alias("distinct_track_id_in_device_listener_id"),
        F.ntile(4).over(w_source_listener_id_by_ms_pla).alias("ms_played_quartile"),
    )
)

screened = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.is_skipped")) & (F.hour(F.col("p.played_at")).between(6, 12)), "left_anti")
)

result = (
    screened
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "last_played_at",
        "track_moods",
        "track_bpm",
        "track_bpm_2",
        "track_duration_sec",
        "track_title",
        "distinct_track_id_in_device_listener_id",
        "ms_played_quartile",
    )
    .orderBy("device")
)
