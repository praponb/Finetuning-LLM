from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_track_bpm_running = Window.partitionBy("device").orderBy(F.col("track_bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device = Window.partitionBy("device")
w_device_by_track_bpm = Window.partitionBy("device").orderBy(F.col("track_bpm").desc(), "id")
w_dedup = Window.partitionBy("listener_id").orderBy(F.col("last_known_played_at").desc(), F.col("id").desc())
w_is_skipped_device = Window.partitionBy("is_skipped", "device")
w_all_by_device_avg_track_bpm = Window.orderBy("device_avg_track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_plays
    .filter(F.col("listener_id") <= 9)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

with_parent = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        F.col("t.moods").alias("track_moods"),
        F.col("t.bpm").alias("track_bpm"),
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

ranked = (
    with_parent
    .select(
        "*",
        F.last("played_at", ignorenulls=True).over(w_device_by_track_bpm_running).alias("last_known_played_at"),
        F.round(F.avg("track_bpm").over(w_device), 2).alias("device_avg_track_bpm"),
        F.dense_rank().over(w_device_by_track_bpm).alias("track_bpm_dense_rank"),
    )
)

unique_rows = (
    ranked
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_counts = (
    unique_rows.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.last_played_at")
)

sequenced = (
    with_counts
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "track_bpm",
        "artist_name",
        "last_known_played_at",
        "last_played_at",
        F.max("device_avg_track_bpm").over(w_is_skipped_device).alias("is_skipped_device_max_device_avg_track_bpm"),
        F.count(F.lit(1)).over(w_is_skipped_device).alias("rows_in_is_skipped_device"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_is_skipped_device), F.lit(0)), 4).alias("track_bpm_share_of_is_skipped_device"),
        F.first("device_avg_track_bpm").over(w_all_by_device_avg_track_bpm).alias("first_device_avg_track_bpm"),
    )
)

with_details = (
    sequenced.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.track_bpm",
        "s.artist_name",
        "s.last_known_played_at",
        "s.last_played_at",
        "s.is_skipped_device_max_device_avg_track_bpm",
        "s.rows_in_is_skipped_device",
        "s.first_device_avg_track_bpm",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
        F.col("t.bpm").alias("track_bpm_2"),
    )
)

result = (
    with_details
    .select(
        "id",
        "listener_id",
        "track_id",
        "artist_name",
        "last_known_played_at",
        "last_played_at",
        "is_skipped_device_max_device_avg_track_bpm",
        "rows_in_is_skipped_device",
        "track_bpm_2",
        F.concat_ws(": ", F.col("artist_name"), F.col("listener_username")).alias("artist_name_listener_username_label"),
    )
    .orderBy(F.col("last_played_at").desc_nulls_first())
)
