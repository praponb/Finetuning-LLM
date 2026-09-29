from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_source_track_id_by_ms_played = Window.partitionBy("source", "track_id").orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_played_at_month_track_id_by = Window.partitionBy(F.month("played_at"), "track_id").orderBy("track_duration_sec", "id")
w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id = Window.partitionBy("track_id")
w_listener_id_by_ms_played_ful = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_track_id_by_ms_played_2 = Window.partitionBy("source", "track_id").orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_all_by_ms_played_full = Window.orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped = Window.partitionBy("is_skipped")
w_all_by_track_duration_sec_vs = Window.orderBy(F.col("track_duration_sec_vs_is_skipped_avg").desc(), "id").rowsBetween(-1, -1)
w_last_known_played_at_year_by = Window.partitionBy(F.year("last_known_played_at")).orderBy("minutes_since_prev_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_track_duration_s = Window.partitionBy("track_id").orderBy("track_duration_sec", "id")
w_track_id_by_track_duration_s_2 = Window.partitionBy("track_id").orderBy("track_duration_sec", "id").rowsBetween(-2, -1)

filtered = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.source_ref",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.moods").alias("track_moods"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "source_ref",
        "track_duration_sec",
        "track_moods",
        F.nth_value("track_duration_sec", 2).over(w_all_by_played_at_full).alias("second_track_duration_sec"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_listener_id_by_ms_played))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.round(F.avg("ms_played").over(w_source_track_id_by_ms_played), 2).alias("ms_played_avg_7"),
        F.row_number().over(w_played_at_month_track_id_by).alias("rn"),
        F.last("played_at", ignorenulls=True).over(w_all_by_ms_played_running).alias("last_known_played_at"),
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id"),
        F.last("ms_played").over(w_listener_id_by_ms_played_ful).alias("last_ms_played"),
        F.row_number().over(w_source_track_id_by_ms_played_2).alias("ms_played_seq"),
        F.ntile(10).over(w_source_track_id_by_ms_played_2).alias("ms_played_decile"),
        F.nth_value("track_duration_sec", 2).over(w_all_by_ms_played_full).alias("second_track_duration_sec_2"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_is_skipped), 2).alias("track_duration_sec_vs_is_skipped_avg"),
    )
)

first_rows = metrics.filter(F.col("rn") == 1)

sequenced = (
    first_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "second_track_duration_sec",
        "minutes_since_prev_played_at",
        "rn",
        "last_known_played_at",
        "last_ms_played",
        "ms_played_seq",
        "second_track_duration_sec_2",
        "rows_in_track_id",
        "track_duration_sec_vs_is_skipped_avg",
        F.max("ms_played").over(w_all_by_track_duration_sec_vs).alias("ms_played_max_1_prior"),
        F.last("rows_in_track_id", ignorenulls=True).over(w_last_known_played_at_year_by).alias("last_known_rows_in_track_id"),
        F.round(F.cume_dist().over(w_track_id_by_track_duration_s), 3).alias("track_duration_sec_cume_dist"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id), F.lit(0)), 4).alias("ms_played_share_of_track_id"),
        F.round(F.avg("ms_played_avg_7").over(w_track_id_by_track_duration_s_2), 2).alias("ms_played_avg_7_avg_2_prior"),
        F.round(F.col("last_ms_played") / F.nullif(F.sum("last_ms_played").over(w_track_id), F.lit(0)), 4).alias("last_ms_played_share_of_track_id"),
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id_2"),
        F.round(F.avg("ms_played_seq").over(w_is_skipped), 2).alias("is_skipped_avg_ms_played_seq"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "second_track_duration_sec",
        "rn",
        "last_known_played_at",
        "last_ms_played",
        "ms_played_seq",
        "second_track_duration_sec_2",
        "rows_in_track_id",
        "track_duration_sec_vs_is_skipped_avg",
        "ms_played_max_1_prior",
        "last_known_rows_in_track_id",
        "track_duration_sec_cume_dist",
        "ms_played_share_of_track_id",
        "last_ms_played_share_of_track_id",
        "distinct_listener_id_in_track_id_2",
        F.when(F.col("rows_in_track_id") >= 16, "HIGH").when(F.col("rows_in_track_id") >= 8, "MEDIUM").otherwise("LOW").alias("rows_in_track_id_tier"),
        (F.col("ms_played") - F.col("ms_played_max_1_prior")).alias("ms_played_minus_ms_played_max_1_prior"),
    )
    .orderBy(F.col("last_ms_played_share_of_track_id").asc(), "id")
    .limit(10)
)
