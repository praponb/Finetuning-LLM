from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_track_bpm = Window.orderBy("track_bpm", "id")
w_track_id_track_release_date = Window.partitionBy("track_id", F.date_trunc("month", "track_release_date")).orderBy(F.col("track_release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_track_release_date_2 = Window.partitionBy("track_id", F.date_trunc("month", "track_release_date")).orderBy(F.col("track_release_date").desc_nulls_first())
w_source = Window.partitionBy("source")
w_track_id_track_release_date_3 = Window.partitionBy("track_id", F.date_trunc("month", "track_release_date")).orderBy(F.col("track_release_date").desc_nulls_first(), "id")
w_listener_id_by_listener_sign = Window.partitionBy("listener_id").orderBy(F.col("listener_signup_date").desc())
w_all_by_track_bpm_running = Window.orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_artist_id_track_bpm_de = Window.partitionBy("track_artist_id", "track_bpm_dense_rank")
w_listener_id_track_release_da = Window.partitionBy("listener_id", "track_release_date_dense_rank").orderBy("days_since_track_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_track_release_da_2 = Window.partitionBy("listener_id", "track_release_date_dense_rank").orderBy("days_since_track_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_running_ms_played_tie = Window.orderBy("running_ms_played")
w_track_bpm_dense_rank_by_dist = Window.partitionBy("track_bpm_dense_rank").orderBy("distinct_track_id_in_source", "id")
w_listener_id = Window.partitionBy("listener_id")

cleaned = (
    df_plays
    .filter((F.col("track_id") >= 15) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.substring(F.trim(F.col("source")), 1, 3).alias("source_prefix"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("t.artist_id").isNotNull())
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.source",
        "s.device_slug",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("l.username").alias("listener_username"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

ranked = (
    joined
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "track_bpm",
        "track_release_date",
        "track_artist_id",
        "listener_username",
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_track_bpm)).alias("ms_played_change"),
        F.ntile(5).over(w_all_by_track_bpm).alias("track_bpm_quintile"),
        F.max("track_bpm").over(w_track_id_track_release_date).alias("max_track_bpm_so_far"),
        F.dense_rank().over(w_track_id_track_release_date_2).alias("track_release_date_dense_rank"),
        F.round(F.col("track_bpm") - F.avg("track_bpm").over(w_source), 2).alias("track_bpm_vs_source_avg"),
        F.ntile(3).over(w_track_id_track_release_date_3).alias("track_release_date_tercile"),
        F.round(F.cume_dist().over(w_listener_id_by_listener_sign), 3).alias("listener_signup_date_cume_dist"),
        F.sum("ms_played").over(w_all_by_track_bpm_running).alias("running_ms_played"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_track_id_track_release_date_3))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.size(F.collect_set("track_id").over(w_source)).alias("distinct_track_id_in_source"),
        F.dense_rank().over(w_all_by_track_bpm).alias("track_bpm_dense_rank"),
        F.min("ms_played").over(w_all_by_track_bpm_running).alias("min_ms_played_so_far"),
        F.round(F.avg("ms_played").over(w_source), 2).alias("source_avg_ms_played"),
        F.round(F.avg("track_bpm").over(w_source), 2).alias("source_avg_track_bpm"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2).alias("ms_played_vs_source_avg"),
    )
)

enriched = (
    ranked
    .filter(F.col("max_track_bpm_so_far") < 1626)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "track_bpm",
        "track_artist_id",
        "listener_username",
        "ms_played_change",
        "max_track_bpm_so_far",
        "track_release_date_dense_rank",
        "listener_signup_date_cume_dist",
        "running_ms_played",
        "minutes_since_prev_played_at",
        "distinct_track_id_in_source",
        "track_bpm_dense_rank",
        "min_ms_played_so_far",
        "source_avg_ms_played",
        "source_avg_track_bpm",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.add_months(F.col("track_release_date"), 1).alias("track_release_date_plus_1m"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("track_release_date")).alias("days_since_track_release_date"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.min("min_ms_played_so_far").over(w_track_artist_id_track_bpm_de).alias("track_artist_id_track_bpm_dense_rank_min_min_ms_"),
        F.count(F.lit(1)).over(w_listener_id_track_release_da).alias("running_count"),
        F.round(F.col("source_avg_track_bpm") - F.avg("source_avg_track_bpm").over(w_track_artist_id_track_bpm_de), 2).alias("source_avg_track_bpm_vs_track_artist_id_track_bp"),
        F.last("distinct_track_id_in_source").over(w_listener_id_track_release_da_2).alias("last_distinct_track_id_in_source"),
        F.max("source_avg_track_bpm").over(w_track_artist_id_track_bpm_de).alias("track_artist_id_track_bpm_dense_rank_max_source_"),
        F.rank().over(w_all_by_running_ms_played_tie).alias("running_ms_played_rank"),
        F.lead("max_track_bpm_so_far", 1, 0).over(w_track_bpm_dense_rank_by_dist).alias("next_max_track_bpm_so_far"),
    )
)

per_group = (
    sequenced
    .groupBy(
        F.year("track_release_date_plus_1m").alias("track_release_date_plus_1m_year"),
        "listener_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("listener_username")).alias("listener_username_list"),
        F.round(F.stddev("track_artist_id_track_bpm_dense_rank_max_source_"), 2).alias("stddev_track_artist_id_track_bpm_dense_rank_max_"),
        F.round(F.sum("minutes_since_prev_played_at"), 2).alias("total_minutes_since_prev_played_at"),
    )
)

sequenced_2 = (
    per_group
    .withColumn("listener_id_max_distinct_ids", F.max("distinct_ids").over(w_listener_id))
    .withColumn("listener_id_avg_distinct_ids", F.round(F.avg("distinct_ids").over(w_listener_id), 2))
)

result = (
    sequenced_2
    .filter(F.col("listener_id_max_distinct_ids") <= 55)
    .select(
        "track_release_date_plus_1m_year",
        "distinct_ids",
        "stddev_track_artist_id_track_bpm_dense_rank_max_",
        "total_minutes_since_prev_played_at",
        "listener_id_max_distinct_ids",
        "listener_id_avg_distinct_ids",
        F.when(F.col("distinct_ids") >= 7, "A").when(F.col("distinct_ids") >= 4, "B").otherwise("C").alias("distinct_ids_tier"),
    )
    .orderBy(F.col("distinct_ids_tier").asc_nulls_last())
)
