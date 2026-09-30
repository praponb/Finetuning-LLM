from pyspark.sql import functions as F

in_window = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.ms_played").alias("prior_ms_played_3d"),
    )
)

combined_details_listener = (
    in_window.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.plan") == "free")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_3d",
        F.col("l.plan").alias("listener_plan"),
    )
)

with_activity = (
    combined_details_listener.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (~F.col("p.is_skipped")), "left_anti")
)

detail_rows_plays = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device").isNotNull()), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.listener_plan",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    detail_rows_plays
    .cube("play_track_id", "play_is_skipped", "play_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("listener_plan")).alias("listener_plan_list"),
        F.countDistinct("play_device").alias("distinct_play_devices"),
    )
    .orderBy(
        F.col("play_track_id").asc_nulls_last(),
        F.col("play_is_skipped").asc_nulls_last(),
        F.col("play_id").asc_nulls_last(),
    )
)
