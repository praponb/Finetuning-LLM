from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), F.col("id").desc())
w_play_id_play_device = Window.partitionBy("play_id", "play_device")
w_play_id_device_by_source_len = Window.partitionBy("play_id", "device").orderBy("source_len", "id", "play_id").rowsBetween(-6, -1)
w_all_by_track_bpm_full = Window.orderBy(F.col("track_bpm").desc(), "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_plays = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.when(F.col("device").isin("car", "phone"), "group_a").when(F.col("device") == "desktop", "group_b").otherwise("other").alias("device_group"),
    )
)

unique_rows = (
    valid_plays
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

qualified = (
    unique_rows.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.duration_sec") >= 184), "left_semi")
)

with_children_plays = (
    qualified.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source_len",
        "s.device_group",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.source").alias("play_source"),
    )
)

combined_details = (
    with_children_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source_len",
        "s.device_group",
        "s.play_id",
        "s.play_ms_played",
        "s.play_device",
        "s.play_source",
        F.col("t.bpm").alias("track_bpm"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

ranked = (
    combined_details
    .select(
        "id",
        "track_id",
        "device",
        "source_len",
        "play_id",
        "play_device",
        "play_source",
        "track_bpm",
        "listener_signup_date",
        F.sum("source_len").over(w_play_id_play_device).alias("play_id_play_device_sum_source_len"),
        F.max("play_ms_played").over(w_play_id_device_by_source_len).alias("play_ms_played_max_6_prior"),
        F.nth_value("listener_signup_date", 2).over(w_all_by_track_bpm_full).alias("second_listener_signup_date"),
        F.count(F.lit(1)).over(w_play_id_play_device).alias("rows_in_play_id_play_device"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_play_id_play_device), F.lit(0)), 4).alias("track_bpm_share_of_play_id_play_device"),
    )
)

derived = (
    ranked
    .select(
        "*",
        F.date_format(F.col("listener_signup_date"), "yyyy-MM-dd").alias("listener_signup_date_label"),
        F.abs(F.col("track_bpm") - 142).alias("track_bpm_distance"),
    )
)

result = (
    derived
    .select(
        "id",
        "track_id",
        "play_id",
        "play_device",
        "play_source",
        "track_bpm",
        "listener_signup_date",
        "play_id_play_device_sum_source_len",
        "play_ms_played_max_6_prior",
        "second_listener_signup_date",
        "track_bpm_share_of_play_id_play_device",
        "listener_signup_date_label",
        "track_bpm_distance",
        F.round(F.col("track_bpm_distance") / F.nullif(F.col("source_len"), F.lit(0)), 3).alias("track_bpm_distance_per_source_len"),
        F.initcap(F.trim(F.col("play_device"))).alias("play_device_clean"),
    )
    .orderBy(F.col("play_device_clean").desc(), "play_ms_played_max_6_prior")
)
