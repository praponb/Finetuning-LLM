from pyspark.sql import functions as F

covered = (
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
    covered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.prior_rows_3d",
        F.col("l.username").alias("listener_username"),
        F.col("l.plan").alias("listener_plan"),
    )
)

line_level_plays = (
    combined_details_listener.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device").isin("speaker", "car")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.listener_username",
        "s.listener_plan",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
    )
)

per_group = (
    line_level_plays
    .groupBy("listener_id")
    .agg(
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("listener_username"))).like("E%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_listener_username"),
        F.count("*").alias("n_rows"),
        F.countDistinct("play_source").alias("distinct_play_sources"),
        F.countDistinct("play_id").alias("distinct_play_ids"),
        F.count(F.when(F.upper(F.trim(F.col("listener_username"))).like("E%"), 1)).alias("n_upper_trim_listener_username"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    per_group
    .select(
        "listener_id",
        "n_rows",
        "distinct_play_sources",
        "distinct_play_ids",
        "n_upper_trim_listener_username",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
    )
    .orderBy(F.col("distinct_play_sources").asc(), "listener_id")
    .limit(10)
)
