from pyspark.sql import functions as F

base = (
    df_plays
    .filter((~F.col("source").isin("radio", "album")) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.min("played_at").alias("first_played_at"),
    )
)

summarized_rows = (
    base.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        "p.first_played_at",
    )
)

labeled = (
    summarized_rows
    .filter(~F.col("is_skipped"))
    .select(
        "*",
        F.least(F.col("ms_played"), F.col("max_play_ms_played")).alias("least_ms_played_max_play_ms_played"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_stats = (
    labeled.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.last_played_at")
)

labeled_2 = (
    with_stats
    .filter(F.col("last_played_at") < F.lit("2025-02-05 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

summary = (
    labeled_2
    .groupBy(
        F.to_date("first_played_at").alias("first_played_at_day"),
        "source",
        "device",
    )
    .agg(
        F.percentile_approx("least_ms_played_max_play_ms_played", 0.5).alias("median_least_ms_played_max_play_ms_played"),
        F.max("last_played_at").alias("last_last_played_at"),
        F.min("least_ms_played_max_play_ms_played").alias("min_least_ms_played_max_play_ms_played"),
        F.min("first_played_at").alias("first_first_played_at"),
    )
)

result = (
    summary
    .select(
        "first_played_at_day",
        "source",
        "min_least_ms_played_max_play_ms_played",
        "median_least_ms_played_max_play_ms_played",
    )
    .unpivot(
        ids=["first_played_at_day", "source"],
        values=["min_least_ms_played_max_play_ms_played", "median_least_ms_played_max_play_ms_played"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
