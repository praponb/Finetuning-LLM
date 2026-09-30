from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_is_explicit_track_id_b = Window.partitionBy("track_is_explicit", "track_id").orderBy("ms_played", "id")
w_source = Window.partitionBy("source")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_all_by_played_at_full = Window.orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_is_explicit_track_id_b_2 = Window.partitionBy("track_is_explicit", "track_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at = Window.orderBy("played_at", "id")
w_track_id = Window.partitionBy("track_id")
w_all_by_ms_played_2 = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_listener_id_by_ms_played_pri = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(-3, -1)
w_all_by_ms_played_full = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_ms_played_run = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_ms_played_tie = Window.partitionBy("listener_id").orderBy("ms_played")
w_listener_id = Window.partitionBy("listener_id")
w_ms_played_rank_listener_id_b = Window.partitionBy("ms_played_rank", "listener_id").orderBy("second_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_distinct_track_id_in = Window.orderBy("distinct_track_id_in_source", "id")

scoped = (
    df_plays
    .filter(F.col("track_id") >= 16)
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

with_details = (
    scoped.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.source",
        F.col("t.is_explicit").alias("track_is_explicit"),
        "a.artist_name",
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.lead("played_at").over(w_track_is_explicit_track_id_b).alias("next_played_at"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.row_number().over(w_all_by_ms_played).alias("ms_played_seq"),
        F.nth_value("played_at", 2).over(w_all_by_played_at_full).alias("second_played_at"),
        F.last("played_at").over(w_track_is_explicit_track_id_b_2).alias("last_played_at"),
        F.dense_rank().over(w_all_by_played_at).alias("played_at_dense_rank"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id), 2).alias("ms_played_vs_track_id_avg"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_ms_played_2))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.size(F.collect_set("track_id").over(w_source)).alias("distinct_track_id_in_source"),
        F.round(F.avg("ms_played").over(w_track_id), 2).alias("track_id_avg_ms_played"),
        F.round(F.avg("ms_played").over(w_listener_id_by_ms_played_pri), 2).alias("ms_played_avg_3_prior"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id), F.lit(0)), 4).alias("ms_played_share_of_track_id"),
        F.first("ms_played").over(w_all_by_ms_played_full).alias("first_ms_played"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.min("ms_played").over(w_listener_id_by_ms_played_run).alias("min_ms_played_so_far"),
        F.rank().over(w_listener_id_by_ms_played_tie).alias("ms_played_rank"),
    )
)

with_trends = (
    ranked
    .select(
        "*",
        F.sum("ms_played_avg_3_prior").over(w_source).alias("source_sum_ms_played_avg_3_prior"),
        F.max("rows_in_track_id").over(w_listener_id).alias("listener_id_max_rows_in_track_id"),
        F.size(F.collect_set("track_id").over(w_listener_id)).alias("distinct_track_id_in_listener_id"),
        F.size(F.collect_set("track_id").over(w_source)).alias("distinct_track_id_in_source_2"),
        F.max("distinct_track_id_in_source").over(w_ms_played_rank_listener_id_b).alias("max_distinct_track_id_in_source_so_far"),
        F.ntile(4).over(w_all_by_distinct_track_id_in).alias("distinct_track_id_in_source_quartile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "track_is_explicit",
        "artist_name",
        "next_played_at",
        "ms_played_seq",
        "second_played_at",
        "played_at_dense_rank",
        "ms_played_vs_track_id_avg",
        "minutes_since_prev_played_at",
        "distinct_track_id_in_source",
        "track_id_avg_ms_played",
        "ms_played_avg_3_prior",
        "ms_played_share_of_track_id",
        "rows_in_track_id",
        "ms_played_rank",
        "source_sum_ms_played_avg_3_prior",
        "distinct_track_id_in_listener_id",
        "distinct_track_id_in_source_2",
        "max_distinct_track_id_in_source_so_far",
        F.concat(F.col("artist_name"), F.lit("#"), F.col("ms_played_rank").cast("string")).alias("artist_name_ref"),
        F.when(F.col("source").isin("search", "album"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
    )
)
