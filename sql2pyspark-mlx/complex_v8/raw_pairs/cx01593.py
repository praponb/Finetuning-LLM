from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("track_id") <= 20))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

with_counts = (
    valid_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.first_played_at")
)

totals = (
    with_counts
    .groupBy("device_len", "device", "listener_id")
    .agg(
        F.min("ms_played").alias("min_ms_played"),
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.round(F.sum("ms_played") / F.nullif(F.sum("device_len"), F.lit(0)), 3).alias("ms_played_to_device_len_ratio"),
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.max("ms_played").alias("max_ms_played"),
        F.sum("ms_played").alias("sum_ms_played_check"),
    )
    .filter(F.col("sum_ms_played_check") > 56848)
    .drop("sum_ms_played_check")
)

with_details_listener = (
    totals.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.size(F.col("l.interests")) >= 1)
    .select(
        "s.device_len",
        "s.device",
        "s.listener_id",
        "s.min_ms_played",
        "s.stddev_ms_played",
        "s.ms_played_to_device_len_ratio",
        "s.n_is_skipped_true",
        "s.distinct_track_ids",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

labeled = (
    with_details_listener
    .select(
        "*",
        (F.col("stddev_ms_played") + F.col("min_ms_played")).alias("stddev_ms_played_plus_min_ms_played"),
        F.greatest(F.col("n_is_skipped_true"), F.col("min_ms_played")).alias("greatest_n_is_skipped_true_min_ms_played"),
    )
)

qualified = (
    labeled.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year") > 1968) & (F.upper(F.trim(F.col("l.username"))).like("G%")), "left_anti")
)

result = (
    qualified
    .select(
        "device_len",
        "listener_id",
        "min_ms_played",
        "n_is_skipped_true",
        "distinct_track_ids",
        "listener_interests",
        "listener_birth_year",
        "stddev_ms_played_plus_min_ms_played",
        "greatest_n_is_skipped_true_min_ms_played",
        F.round(F.col("stddev_ms_played") / F.nullif(F.col("min_ms_played"), F.lit(0)), 3).alias("stddev_ms_played_per_min_ms_played"),
    )
    .orderBy("n_is_skipped_true", F.col("device_len").desc())
)
