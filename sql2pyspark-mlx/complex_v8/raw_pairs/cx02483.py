from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_played_at_month_track_id_by = Window.partitionBy(F.date_trunc("month", "played_at"), "track_id").orderBy(F.col("source_len").desc(), "id")
w_is_skipped_device_by_played = Window.partitionBy("is_skipped", "device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_month_track_id_by_2 = Window.partitionBy(F.date_trunc("month", "played_at"), "track_id").orderBy(F.col("source_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_len_by_played_at_movi = Window.partitionBy("source_len").orderBy("played_at", "id").rowsBetween(-3, Window.currentRow)
w_source_len_by_played_at = Window.partitionBy("source_len").orderBy("played_at", "id")
w_is_skipped_by_source_len_run = Window.partitionBy("is_skipped").orderBy(F.col("source_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_len = Window.partitionBy("source_len")
w_device_by_second_played_at = Window.partitionBy("device").orderBy(F.col("second_played_at").asc_nulls_last(), "id")
w_all_by_n_source_in_search_pl = Window.orderBy("n_source_in_search_playlist_al", "second_played_at_2_date_month", "track_id", "rows_in_source_len_tier").rowsBetween(-1, -1)
w_all_by_n_source_in_search_pl_2 = Window.orderBy("n_source_in_search_playlist_al", "second_played_at_2_date_month", "track_id", "rows_in_source_len_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_plays
    .filter(F.month(F.col("played_at")).isin(2, 3))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.round(F.col("source_len") / F.nullif(F.sum("source_len").over(w_is_skipped), F.lit(0)), 4).alias("source_len_share_of_is_skipped"),
        (F.col("source_len") - F.lag("source_len").over(w_played_at_month_track_id_by)).alias("source_len_change"),
        F.nth_value("played_at", 2).over(w_is_skipped_device_by_played).alias("second_played_at"),
        F.nth_value("played_at", 2).over(w_played_at_month_track_id_by_2).alias("second_played_at_2"),
        F.sum("source_len").over(w_is_skipped).alias("is_skipped_sum_source_len"),
        F.round(F.avg("source_len").over(w_source_len_by_played_at_movi), 2).alias("source_len_avg_4"),
        F.row_number().over(w_played_at_month_track_id_by).alias("source_len_seq"),
        F.min("source_len").over(w_is_skipped).alias("is_skipped_min_source_len"),
        F.ntile(10).over(w_source_len_by_played_at).alias("played_at_decile"),
        F.last("played_at", ignorenulls=True).over(w_is_skipped_by_source_len_run).alias("last_known_played_at"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "source_len",
        "second_played_at",
        "is_skipped_sum_source_len",
        "source_len_avg_4",
        "is_skipped_min_source_len",
        "played_at_decile",
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label"),
        F.to_date(F.col("second_played_at_2")).alias("second_played_at_2_date"),
        F.concat_ws("|", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

with_windows = (
    annotated
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "source_len",
        "second_played_at",
        "is_skipped_sum_source_len",
        "source_len_avg_4",
        "is_skipped_min_source_len",
        "played_at_decile",
        "second_played_at_2_date",
        "source_device_label",
        F.round(F.avg("is_skipped_sum_source_len").over(w_source_len), 2).alias("source_len_avg_is_skipped_sum_source_len"),
        F.count(F.lit(1)).over(w_source_len).alias("rows_in_source_len"),
        F.round(F.avg("is_skipped_min_source_len").over(w_source_len), 2).alias("source_len_avg_is_skipped_min_source_len"),
        F.round(F.col("source_len") / F.nullif(F.sum("source_len").over(w_source_len), F.lit(0)), 4).alias("source_len_share_of_source_len"),
        F.ntile(5).over(w_device_by_second_played_at).alias("second_played_at_quintile"),
    )
)

per_group = (
    with_windows
    .groupBy(
        F.date_trunc("month", "second_played_at_2_date").alias("second_played_at_2_date_month"),
        "track_id",
        F.when(F.col("rows_in_source_len") >= 16, "A").when(F.col("rows_in_source_len") >= 9, "B").otherwise("C").alias("rows_in_source_len_tier"),
    )
    .agg(
        F.sum(F.when(F.col("source").isin("radio", "playlist"), F.col("source_len"))).alias("source_len_source_in_radio_playlist"),
        F.round(F.avg("source_len_share_of_source_len"), 2).alias("avg_source_len_share_of_source_len"),
        F.count(F.when(F.col("source").isin("search", "playlist", "album"), 1)).alias("n_source_in_search_playlist_al"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.count("*").alias("row_count"),
        F.countDistinct("source").alias("distinct_sources"),
    )
)

metrics_2 = (
    per_group
    .select(
        "second_played_at_2_date_month",
        "track_id",
        "rows_in_source_len_tier",
        "n_source_in_search_playlist_al",
        "source_list",
        "row_count",
        "distinct_sources",
        F.round(F.avg("distinct_sources").over(w_all_by_n_source_in_search_pl), 2).alias("distinct_sources_avg_1_prior"),
        F.first("row_count", ignorenulls=True).over(w_all_by_n_source_in_search_pl_2).alias("first_known_row_count"),
    )
)

result = (
    metrics_2
    .filter(F.col("distinct_sources_avg_1_prior") < 9.1)
    .select(
        "second_played_at_2_date_month",
        "track_id",
        "rows_in_source_len_tier",
        "n_source_in_search_playlist_al",
        "source_list",
        "row_count",
        "distinct_sources_avg_1_prior",
        "first_known_row_count",
        F.lower(F.regexp_replace(F.trim(F.col("rows_in_source_len_tier")), " ", "_")).alias("rows_in_source_len_tier_slug"),
        F.round(F.col("row_count") / F.nullif(F.col("distinct_sources"), F.lit(0)), 3).alias("row_count_per_distinct_sources"),
    )
)
