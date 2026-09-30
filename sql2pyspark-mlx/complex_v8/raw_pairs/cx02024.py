from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter((F.month(F.col("played_at")).isin(1, 3)) | (F.col("source") == "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

stats = (
    prepared
    .groupBy(
        "device",
        "track_id",
        F.date_format("played_at", "yyyy-MM").alias("played_at_ym"),
    )
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.min("played_at").alias("first_played_at"),
        F.sum(F.when(F.col("device").isin("car", "phone"), F.col("is_skipped_flag"))).alias("is_skipped_flag_device_in_car_phone"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
    )
)

exploded_detail_plays = (
    stats.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.device",
        "s.track_id",
        "s.played_at_ym",
        "s.last_played_at",
        "s.first_played_at",
        "s.is_skipped_flag_device_in_car_phone",
        "s.distinct_listener_ids",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    exploded_detail_plays
    .cube("play_listener_id", "device", "is_skipped_flag_device_in_car_phone")
    .agg(
        F.countDistinct("device").alias("distinct_devices"),
        F.count("*").alias("row_count"),
        F.max("distinct_listener_ids").alias("max_distinct_listener_ids"),
    )
    .select(
        "play_listener_id",
        F.coalesce(F.col("device"), F.lit("ALL")).alias("device"),
        "is_skipped_flag_device_in_car_phone",
        "distinct_devices",
        "row_count",
        "max_distinct_listener_ids",
    )
    .orderBy(
        F.col("play_listener_id").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
        F.col("is_skipped_flag_device_in_car_phone").asc_nulls_last(),
    )
)
