from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_track_duration_sec = Window.orderBy(F.col("track_duration_sec").desc(), "id")
w_track_id = Window.partitionBy("track_id")
w_device = Window.partitionBy("device")
w_track_id_listener_signup_dat = Window.partitionBy("track_id", F.date_trunc("month", "listener_signup_date")).orderBy("track_duration_sec", "id")
w_track_id_listener_signup_dat_2 = Window.partitionBy("track_id", F.date_trunc("month", "listener_signup_date")).orderBy("track_duration_sec", "id").rowsBetween(-6, Window.currentRow)

prepared_plays = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

enriched_listener = (
    prepared_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.*",
        F.col("l.country").alias("listener_country"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.moods").alias("track_moods"),
    )
)

with_trends = (
    enriched_listener
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "device_or_unknown",
        "listener_plan",
        "track_duration_sec",
        F.lag("track_duration_sec").over(w_all_by_track_duration_sec).alias("prev_track_duration_sec"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.size(F.collect_set("source").over(w_device)).alias("distinct_source_in_device"),
        F.ntile(10).over(w_track_id_listener_signup_dat).alias("track_duration_sec_decile"),
        F.round(F.col("track_duration_sec") / F.nullif(F.sum("track_duration_sec").over(w_track_id), F.lit(0)), 4).alias("track_duration_sec_share_of_track_id"),
        F.lag("listener_signup_date").over(w_all_by_track_duration_sec).alias("prev_listener_signup_date"),
        F.max("ms_played").over(w_device).alias("device_max_ms_played"),
        F.round(F.avg("ms_played").over(w_track_id_listener_signup_dat_2), 2).alias("ms_played_avg_7"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "track_id",
        "device",
        "source",
        "device_or_unknown",
        "listener_plan",
        "track_duration_sec",
        "prev_track_duration_sec",
        "rows_in_track_id",
        "track_duration_sec_share_of_track_id",
        "prev_listener_signup_date",
        "device_max_ms_played",
        F.abs(F.col("track_duration_sec") - 344).alias("track_duration_sec_distance"),
    )
    .orderBy("device_or_unknown")
)
