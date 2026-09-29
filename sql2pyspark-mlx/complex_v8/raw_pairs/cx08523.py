from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_listener_id_by_ms_p = Window.partitionBy("track_id", "listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_by_ms_played_prior_2 = Window.partitionBy("source").orderBy("ms_played", "id").rowsBetween(-2, -1)

base = (
    df_plays
    .filter(F.col("device") != "tv")
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        F.last("ms_played").over(w_track_id_listener_id_by_ms_p).alias("last_ms_played"),
        F.max("ms_played").over(w_source_by_ms_played_prior_2).alias("ms_played_max_2_prior"),
    )
)

per_group = (
    with_windows
    .groupBy("is_skipped", "track_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("n_is_skipped_false"),
        F.percentile_approx("ms_played", 0.5).alias("median_ms_played"),
        F.round(F.sum("ms_played_max_2_prior") / F.nullif(F.sum("last_ms_played"), F.lit(0)), 3).alias("ms_played_max_2_prior_to_last_ms_played_ratio"),
    )
)

enriched = (
    per_group
    .select(
        "*",
        F.when(F.col("median_ms_played") >= 281500, "large").when(F.col("median_ms_played") >= 182750, "medium").otherwise("small").alias("median_ms_played_tier"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_stats = (
    enriched.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.last_played_at")
)

derived = (
    with_stats
    .select(
        "is_skipped",
        "track_id",
        "distinct_ids",
        "n_is_skipped_false",
        "ms_played_max_2_prior_to_last_ms_played_ratio",
        "median_ms_played_tier",
        F.quarter(F.col("last_played_at")).alias("last_played_at_quarter"),
    )
)

qualified = (
    derived
    .crossJoin(derived.agg(F.min("n_is_skipped_false").alias("overall_min_n_is_skipped_false")))
    .filter(F.col("n_is_skipped_false") > F.col("overall_min_n_is_skipped_false"))
    .drop("overall_min_n_is_skipped_false")
)

result = (
    qualified
    .select(
        "is_skipped",
        "track_id",
        "n_is_skipped_false",
        "last_played_at_quarter",
        "distinct_ids",
    )
    .unpivot(
        ids=["is_skipped", "track_id"],
        values=["n_is_skipped_false", "last_played_at_quarter", "distinct_ids"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("is_skipped", "track_id", "metric")
)
