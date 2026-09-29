from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
    )
)

with_parent_listener = (
    valid.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country").isNotNull()), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        F.col("l.username").alias("listener_username"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

combined_details = (
    with_parent_listener.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.size(F.col("l.interests")) >= 2), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.listener_username",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

detail_rows_plays = (
    combined_details.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.listener_signup_date",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
    )
)

stats = (
    detail_rows_plays
    .groupBy("track_id")
    .agg(
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.array_sort(F.collect_set("play_device")).alias("play_device_list"),
        F.sum(F.when(F.col("play_device") == "phone", 1).otherwise(0)).alias("n_play_device_phone"),
        F.sum(F.when(F.col("device") == "phone", 1).otherwise(0)).alias("n_device_phone"),
        F.max("listener_signup_date").alias("last_listener_signup_date"),
    )
)

result = (
    stats
    .filter(F.size(F.col("play_device_list")) >= 1)
    .select(
        "track_id",
        "device_list",
        "play_device_list",
        "last_listener_signup_date",
        F.greatest(F.col("n_device_phone"), F.col("n_play_device_phone")).alias("greatest_n_device_phone_n_play_device_phone"),
        F.coalesce(F.col("n_device_phone"), F.lit(0)).alias("n_device_phone_filled"),
    )
    .orderBy(F.col("last_listener_signup_date").desc_nulls_first())
)
