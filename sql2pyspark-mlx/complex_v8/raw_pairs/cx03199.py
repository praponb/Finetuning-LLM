from pyspark.sql import functions as F

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

totals = (
    scoped_plays
    .filter(F.col("device").isin("desktop", "tv"))
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("n_is_skipped_true"),
        F.countDistinct("source_or_unknown").alias("distinct_source_or_unknowns"),
        F.count(F.when(F.col("device").isin("desktop", "phone", "tv"), 1)).alias("n_device_in_desktop_phone_tv"),
    )
    .filter(F.col("row_count") >= 2)
)

screened = (
    totals.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.hour(F.col("p.played_at")).between(10, 16)) & (~F.col("p.is_skipped")), "left_anti")
    .filter(F.col("listener_id").isNotNull())
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count("*").alias("play_count_2"),
        F.count("*").alias("play_count_3"),
    )
)

summarized_rows = (
    screened.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
        F.coalesce(F.col("p.play_count_3"), F.lit(0)).alias("play_count_3"),
    )
)

tagged = (
    summarized_rows
    .filter(F.col("play_count_3") > 3)
    .select(
        "listener_id",
        "row_count",
        "distinct_listener_ids",
        "distinct_source_or_unknowns",
        "n_device_in_desktop_phone_tv",
        "play_count",
        "play_count_2",
        F.coalesce(F.col("play_count_2"), F.lit(0)).alias("play_count_2_filled"),
        (F.col("play_count") + F.col("play_count_3")).alias("play_count_plus_play_count_3"),
        F.coalesce(F.col("distinct_listener_ids"), F.lit(0)).alias("distinct_listener_ids_filled"),
    )
)

result = (
    tagged
    .select(
        "listener_id",
        "distinct_listener_ids",
        "distinct_source_or_unknowns",
        "n_device_in_desktop_phone_tv",
        "play_count",
        "play_count_2",
        F.abs(F.col("distinct_listener_ids") - 5).alias("distinct_listener_ids_distance"),
    )
)
