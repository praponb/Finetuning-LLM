from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_ms_played_runn = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id = Window.partitionBy("track_id")
w_source_listener_id_by_played = Window.partitionBy("source", "listener_id").orderBy("played_at", "id").rowsBetween(-3, -1)
w_is_skipped_track_id_by_days = Window.partitionBy("is_skipped", "track_id").orderBy("days_since_played_at", "id").rowsBetween(-3, Window.currentRow)
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id")
w_track_id_ms_played_tier = Window.partitionBy("track_id", "ms_played_tier")
w_listener_id_ms_played_tier_b = Window.partitionBy("listener_id", "ms_played_tier").orderBy(F.col("ms_played_avg_3_prior").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_ms_played_quintile_by_ms_pla = Window.partitionBy("ms_played_quintile").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_plays = (
    df_plays
    .filter((F.col("ms_played") >= 71476) | (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 308104, "A").when(F.col("ms_played") >= 168782, "B").otherwise("C").alias("ms_played_tier"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

with_details = (
    prepared_plays.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country") == "TH"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.source",
        "s.ms_played_tier",
        "s.days_since_played_at",
        F.col("l.username").alias("listener_username"),
    )
)

metrics = (
    with_details
    .select(
        "*",
        F.min("days_since_played_at").over(w_is_skipped_by_ms_played_runn).alias("min_days_since_played_at_so_far"),
        F.max("ms_played").over(w_track_id).alias("track_id_max_ms_played"),
        F.round(F.avg("ms_played").over(w_source_listener_id_by_played), 2).alias("ms_played_avg_3_prior"),
        F.count(F.lit(1)).over(w_is_skipped_by_ms_played_runn).alias("running_count"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id), F.lit(0)), 4).alias("ms_played_share_of_track_id"),
        F.sum("days_since_played_at").over(w_is_skipped_track_id_by_days).alias("days_since_played_at_sum_4"),
        F.ntile(5).over(w_is_skipped_by_ms_played).alias("ms_played_quintile"),
    )
)

scored = (
    metrics
    .filter(F.col("days_since_played_at") <= 420)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "ms_played_tier",
        "listener_username",
        "min_days_since_played_at_so_far",
        "track_id_max_ms_played",
        "ms_played_avg_3_prior",
        "running_count",
        "ms_played_share_of_track_id",
        "days_since_played_at_sum_4",
        "ms_played_quintile",
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
    )
)

ranked = (
    scored
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        "listener_username",
        "min_days_since_played_at_so_far",
        "running_count",
        "ms_played_share_of_track_id",
        "days_since_played_at_sum_4",
        "ms_played_quintile",
        "source_ref",
        F.sum("ms_played_avg_3_prior").over(w_track_id_ms_played_tier).alias("track_id_ms_played_tier_sum_ms_played_avg_3_prio"),
        F.max("days_since_played_at_sum_4").over(w_listener_id_ms_played_tier_b).alias("days_since_played_at_sum_4_max_6_prior"),
        F.last("days_since_played_at_sum_4").over(w_ms_played_quintile_by_ms_pla).alias("last_days_since_played_at_sum_4"),
    )
)

result = (
    ranked
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "source",
        "listener_username",
        "min_days_since_played_at_so_far",
        "running_count",
        "ms_played_share_of_track_id",
        "days_since_played_at_sum_4",
        "source_ref",
        "track_id_ms_played_tier_sum_ms_played_avg_3_prio",
        "days_since_played_at_sum_4_max_6_prior",
        "last_days_since_played_at_sum_4",
    )
    .orderBy(F.col("running_count").asc_nulls_last(), "id")
    .limit(5)
)
