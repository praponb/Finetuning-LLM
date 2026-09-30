from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_is_skipped_by_ms_play = Window.partitionBy("source", "is_skipped").orderBy("ms_played", "id")
w_is_skipped_listener_id = Window.partitionBy("is_skipped", "listener_id")
w_source_by_source_len_running = Window.partitionBy("source").orderBy("source_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_source_len_by_ms_pl = Window.partitionBy("track_id", "source_len").orderBy(F.col("ms_played").asc_nulls_last(), "id")

prepared = df_plays.filter(F.col("device") != "phone")

derived = (
    prepared
    .select(
        "*",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

with_windows = (
    derived
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        "source_len",
        F.round(F.cume_dist().over(w_source_is_skipped_by_ms_play), 3).alias("ms_played_cume_dist"),
        F.count(F.lit(1)).over(w_is_skipped_listener_id).alias("rows_in_is_skipped_listener_id"),
        F.size(F.collect_set("track_id").over(w_is_skipped_listener_id)).alias("distinct_track_id_in_is_skipped_listener_id"),
        F.round(F.col("source_len") / F.nullif(F.sum("source_len").over(w_is_skipped_listener_id), F.lit(0)), 4).alias("source_len_share_of_is_skipped_listener_id"),
        F.max("source_len").over(w_source_by_source_len_running).alias("max_source_len_so_far"),
        F.datediff(F.col("played_at_date"), F.lag("played_at_date").over(w_track_id_source_len_by_ms_pl)).alias("days_since_prev_played_at_date"),
    )
)

grouped = (
    with_windows
    .groupBy(
        "source_len",
        F.date_trunc("month", "played_at").alias("played_at_month"),
    )
    .agg(
        F.sum("rows_in_is_skipped_listener_id").alias("total_rows_in_is_skipped_listener_id"),
        F.count("*").alias("row_count"),
        F.max("ms_played").alias("max_ms_played"),
        F.min("played_at").alias("first_played_at"),
        F.count(F.when(F.col("source").isNotNull(), 1)).alias("n_source_is_not_null"),
        F.round(F.sum("source_len_share_of_is_skipped_listener_id") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("source_len_share_of_is_skipped_listener_id_to_ms"),
    )
)

result = (
    grouped
    .filter(F.col("total_rows_in_is_skipped_listener_id").between(31, 96))
    .select(
        "source_len",
        "played_at_month",
        "total_rows_in_is_skipped_listener_id",
        "first_played_at",
        "n_source_is_not_null",
        "source_len_share_of_is_skipped_listener_id_to_ms",
        F.least(F.col("max_ms_played"), F.col("n_source_is_not_null")).alias("least_max_ms_played_n_source_is_not_null"),
        F.date_trunc("hour", F.col("first_played_at")).alias("first_played_at_hour"),
    )
    .orderBy(
        "first_played_at_hour",
        "source_len_share_of_is_skipped_listener_id_to_ms",
    )
)
