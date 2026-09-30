from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_label = Window.partitionBy("is_skipped_label")
w_n_rows_by_median_ms_played_a = Window.partitionBy("n_rows").orderBy(F.col("median_ms_played_adj_play_is_skipped_true").desc_nulls_first(), "is_skipped", "is_skipped_label", "play_listener_id")

recent_plays = (
    df_plays
    .filter((F.col("source") != "radio") & (F.col("ms_played") <= 236512))
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

source_stats = (
    recent_plays
    .groupBy("source")
    .agg(F.percentile_approx("ms_played", 0.5).alias("median_ms_played"))
)

compared = (
    recent_plays.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") >= F.col("g.median_ms_played"))
    .select(
        "s.*",
        "g.median_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.median_ms_played"), 2).alias("ms_played_minus_median"),
    )
)

enriched = (
    compared
    .select(
        "*",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.round(F.col("median_ms_played") * 1.5, 2).alias("median_ms_played_adj"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

detail_rows_plays = (
    enriched.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_minus_median",
        "s.median_ms_played_adj",
        "s.is_skipped_label",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

summary = (
    detail_rows_plays
    .groupBy("is_skipped", "is_skipped_label", "play_listener_id")
    .agg(
        F.max("ms_played").alias("max_ms_played"),
        F.sum(F.when(F.col("play_source") == "playlist", F.col("median_ms_played_adj")).otherwise(0)).alias("median_ms_played_adj_play_source_playlist"),
        F.round(F.sum("median_ms_played_adj") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("median_ms_played_adj_to_ms_played_ratio"),
        F.round(F.stddev("ms_played_minus_median"), 2).alias("stddev_ms_played_minus_median"),
        F.sum(F.when(F.col("play_is_skipped"), F.col("median_ms_played_adj"))).alias("median_ms_played_adj_play_is_skipped_true"),
        F.count("*").alias("n_rows"),
    )
)

metrics = (
    summary
    .select(
        "is_skipped",
        "is_skipped_label",
        "play_listener_id",
        "median_ms_played_adj_play_source_playlist",
        "median_ms_played_adj_to_ms_played_ratio",
        "stddev_ms_played_minus_median",
        "median_ms_played_adj_play_is_skipped_true",
        "n_rows",
        F.size(F.collect_set("play_listener_id").over(w_is_skipped_label)).alias("distinct_play_listener_id_in_is_skipped_label"),
        F.lag("max_ms_played", 2).over(w_n_rows_by_median_ms_played_a).alias("prev2_max_ms_played"),
        F.round(F.col("n_rows") / F.nullif(F.sum("n_rows").over(w_is_skipped_label), F.lit(0)), 4).alias("n_rows_share_of_is_skipped_label"),
    )
)

qualified = (
    metrics.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.play_listener_id")) & (F.col("p.played_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp")), "left_anti")
)

result = (
    qualified
    .groupBy("play_listener_id")
    .pivot("is_skipped_label", ["yes", "no"])
    .agg(F.min("prev2_max_ms_played"))
)
