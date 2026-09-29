from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy("played_at", "id")
w_source = Window.partitionBy("source")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_all_by_played_at_ties = Window.orderBy("played_at")
w_is_skipped_track_id_by_playe = Window.partitionBy("is_skipped", "track_id").orderBy("played_at", "id").rowsBetween(-6, -1)
w_source_played_at_month_by_pl = Window.partitionBy("source", F.date_trunc("month", "played_at")).orderBy(F.col("played_at").desc(), "id")
w_source_played_at_month_by_pl_2 = Window.partitionBy("source", F.date_trunc("month", "played_at")).orderBy(F.col("played_at").desc())
w_source_played_at_month_by_pl_3 = Window.partitionBy("source", F.date_trunc("month", "played_at")).orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_ties = Window.orderBy("ms_played")
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_seq_no_ms_played_dense_rank = Window.partitionBy("seq_no", "ms_played_dense_rank")
w_listener_id_seq_no_by_ms_pla = Window.partitionBy("listener_id", "seq_no").orderBy(F.col("ms_played_vs_source_avg").asc_nulls_last(), "id")
w_all_by_ms_played_quintile = Window.orderBy(F.col("ms_played_quintile").desc_nulls_first(), "id")

cleaned = (
    df_plays
    .filter((F.col("device") == "desktop") & (F.col("source") == "album"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "source",
        F.row_number().over(w_all_by_played_at).alias("played_at_seq"),
        F.sum("ms_played").over(w_source).alias("source_sum_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source), 2).alias("ms_played_vs_source_avg"),
        F.ntile(5).over(w_all_by_ms_played).alias("ms_played_quintile"),
        F.max("ms_played").over(w_is_skipped).alias("is_skipped_max_ms_played"),
        F.rank().over(w_all_by_played_at_ties).alias("played_at_rank"),
        F.sum("ms_played").over(w_is_skipped_track_id_by_playe).alias("ms_played_sum_6_prior"),
        F.ntile(10).over(w_source_played_at_month_by_pl).alias("played_at_decile"),
        F.round(F.percent_rank().over(w_source_played_at_month_by_pl_2), 3).alias("played_at_percent_rank"),
        F.count(F.lit(1)).over(w_source_played_at_month_by_pl_3).alias("running_count"),
        F.dense_rank().over(w_all_by_ms_played_ties).alias("ms_played_dense_rank"),
        F.last("played_at", ignorenulls=True).over(w_all_by_ms_played_running).alias("last_known_played_at"),
        F.row_number().over(w_all_by_ms_played).alias("seq_no"),
        F.round(F.percent_rank().over(w_all_by_played_at_ties), 3).alias("played_at_percent_rank_2"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.count(F.lit(1)).over(w_seq_no_ms_played_dense_rank).alias("rows_in_seq_no_ms_played_dense_rank"),
        F.round(F.col("ms_played_vs_source_avg") - F.avg("ms_played_vs_source_avg").over(w_seq_no_ms_played_dense_rank), 2).alias("ms_played_vs_source_avg_vs_seq_no_ms_played_dens"),
        (F.col("running_count") - F.lag("running_count").over(w_listener_id_seq_no_by_ms_pla)).alias("running_count_change"),
        F.round(F.percent_rank().over(w_all_by_ms_played_quintile), 3).alias("ms_played_quintile_percent_rank"),
        (F.col("ms_played_dense_rank") - F.lag("ms_played_dense_rank").over(w_listener_id_seq_no_by_ms_pla)).alias("ms_played_dense_rank_change"),
        F.size(F.collect_set("source").over(w_seq_no_ms_played_dense_rank)).alias("distinct_source_in_seq_no_ms_played_dense_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("played_at_seq") >= 3)
    .select(
        "id",
        "played_at",
        "ms_played",
        "played_at_seq",
        "ms_played_vs_source_avg",
        "is_skipped_max_ms_played",
        "played_at_rank",
        "played_at_decile",
        "played_at_percent_rank",
        "running_count",
        "ms_played_dense_rank",
        "last_known_played_at",
        "played_at_percent_rank_2",
        "rows_in_seq_no_ms_played_dense_rank",
        "ms_played_vs_source_avg_vs_seq_no_ms_played_dens",
        "ms_played_dense_rank_change",
        "distinct_source_in_seq_no_ms_played_dense_rank",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
    .orderBy(F.col("id").desc(), "played_at")
)
