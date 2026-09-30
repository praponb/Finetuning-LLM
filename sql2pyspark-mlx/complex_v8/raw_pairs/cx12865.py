from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_all_by_track_release_date = Window.orderBy("track_release_date", "id")

valid = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(6, 8))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        "s.device_prefix",
        "s.played_at_label",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "track_id",
        "played_at",
        "source",
        "device_prefix",
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.sum("listener_birth_year").over(w_listener_id).alias("listener_id_sum_listener_birth_year"),
        F.row_number().over(w_all_by_track_release_date).alias("seq_no"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "track_id",
        "played_at",
        "source",
        "device_prefix",
        "rows_in_listener_id",
        "listener_id_sum_listener_birth_year",
        "seq_no",
        F.length(F.trim(F.col("device_prefix"))).alias("device_prefix_len"),
        F.when(F.col("source").isin("playlist", "radio"), "group_a").when(F.col("source") == "search", "group_b").otherwise("other").alias("source_group"),
    )
    .orderBy(F.col("rows_in_listener_id").asc(), F.col("played_at").asc(), "id")
    .limit(10)
)
