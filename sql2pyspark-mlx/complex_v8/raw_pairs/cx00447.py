from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_played_at = Window.partitionBy("is_skipped").orderBy("played_at", "id")
w_track_id = Window.partitionBy("track_id")
w_all_by_ms_played_ties = Window.orderBy(F.col("ms_played").asc_nulls_last())
w_all_by_ms_played = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_track_id_listener_id_by_trac = Window.partitionBy("track_id", "listener_id").orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_listener_id_by_trac_2 = Window.partitionBy("track_id", "listener_id").orderBy("track_bpm")
w_is_skipped_by_played_at_movi = Window.partitionBy("is_skipped").orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_track_id_listener_id_by_trac_3 = Window.partitionBy("track_id", "listener_id").orderBy("track_bpm", "id").rowsBetween(-1, Window.currentRow)
w_track_id_listener_id_by_trac_4 = Window.partitionBy("track_id", "listener_id").orderBy("track_bpm", "id")

filtered = (
    df_plays
    .filter((F.month(F.col("played_at")).isin(2, 3)) & (F.col("device").isin("car", "tv")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.initcap(F.trim(F.col("device"))).alias("device_clean"),
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("l.username").alias("listener_username"),
        F.col("l.country").alias("listener_country"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

sequenced = (
    combined_details
    .select(
        "id",
        "played_at",
        "ms_played",
        "listener_username",
        "listener_country",
        (F.col("ms_played") - F.lag("ms_played").over(w_is_skipped_by_played_at)).alias("ms_played_change"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.rank().over(w_all_by_ms_played_ties).alias("ms_played_rank"),
        F.round(F.col("track_bpm") - F.avg("track_bpm").over(w_track_id), 2).alias("track_bpm_vs_track_id_avg"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_ms_played))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.last("track_release_date").over(w_track_id_listener_id_by_trac).alias("last_track_release_date"),
        F.dense_rank().over(w_is_skipped_by_played_at).alias("played_at_dense_rank"),
        F.round(F.avg("track_bpm").over(w_track_id), 2).alias("track_id_avg_track_bpm"),
        F.dense_rank().over(w_track_id_listener_id_by_trac_2).alias("track_bpm_dense_rank"),
        F.round(F.avg("ms_played").over(w_is_skipped_by_played_at_movi), 2).alias("ms_played_avg_3"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_track_id), F.lit(0)), 4).alias("track_bpm_share_of_track_id"),
        F.max("ms_played").over(w_track_id_listener_id_by_trac_3).alias("ms_played_max_2"),
        F.lag("ms_played", 3).over(w_track_id_listener_id_by_trac_4).alias("prev3_ms_played"),
        F.round(F.percent_rank().over(w_is_skipped_by_played_at), 3).alias("played_at_percent_rank"),
    )
)

tagged = (
    sequenced
    .filter(F.col("track_bpm_share_of_track_id").between(0.1, 0.9))
    .select(
        "*",
        F.greatest(F.col("track_id_avg_track_bpm"), F.col("played_at_dense_rank")).alias("greatest_track_id_avg_track_bpm_played_at_dense_"),
        F.greatest(F.col("rows_in_track_id"), F.col("prev3_ms_played")).alias("greatest_rows_in_track_id_prev3_ms_played"),
        F.greatest(F.col("prev3_ms_played"), F.col("track_id_avg_track_bpm")).alias("greatest_prev3_ms_played_track_id_avg_track_bpm"),
    )
)

stats = (
    tagged
    .filter(F.col("ms_played_avg_3") <= 242000.0)
    .groupBy(
        "played_at_dense_rank",
        "track_bpm_dense_rank",
        F.when(F.col("ms_played") >= 245877, "large").when(F.col("ms_played") >= 166517, "medium").otherwise("small").alias("ms_played_tier"),
    )
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("listener_username"))).like("G%"), 1).otherwise(0)).alias("n_upper_trim_listener_username"),
        F.array_sort(F.collect_set("listener_username")).alias("listener_username_list"),
        F.count(F.when(F.col("listener_country").isNotNull(), 1)).alias("n_listener_country_is_not_null"),
    )
)

result = (
    stats
    .select(
        "played_at_dense_rank",
        "track_bpm_dense_rank",
        "ms_played_tier",
        "n_upper_trim_listener_username",
        "listener_username_list",
        "n_listener_country_is_not_null",
        F.coalesce(F.col("played_at_dense_rank"), F.lit(0)).alias("played_at_dense_rank_filled"),
        F.least(F.col("played_at_dense_rank"), F.col("track_bpm_dense_rank")).alias("least_played_at_dense_rank_track_bpm_dense_rank"),
    )
    .orderBy(
        F.col("n_listener_country_is_not_null").asc_nulls_last(),
        "played_at_dense_rank",
        "track_bpm_dense_rank",
        "ms_played_tier",
    )
    .limit(10)
)
