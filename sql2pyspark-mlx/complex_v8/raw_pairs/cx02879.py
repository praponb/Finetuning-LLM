from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_played_at_month_by_pl = Window.partitionBy("source", F.date_trunc("month", "played_at")).orderBy("played_at")
w_source_by_played_at_full = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_label_played_at_m = Window.partitionBy("is_skipped_label", F.month("played_at"))
w_source_by_played_at_moving_2 = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_played_at_running = Window.orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_by_played_at_running = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at = Window.orderBy(F.col("played_at").desc_nulls_first(), "id")
w_all_by_played_at_full = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_to_date_last_playe = Window.partitionBy(F.to_date("played_at"), F.year("last_played_at"))
w_all_by_is_skipped_label_play = Window.orderBy(F.col("is_skipped_label_played_at_month_avg_ms_played").desc(), "id")

prepared_plays = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

with_windows = (
    prepared_plays
    .select(
        "id",
        "played_at",
        "source",
        F.round(F.cume_dist().over(w_source_played_at_month_by_pl), 3).alias("played_at_cume_dist"),
        F.nth_value("ms_played", 2).over(w_source_by_played_at_full).alias("second_ms_played"),
        F.round(F.avg("ms_played").over(w_is_skipped_label_played_at_m), 2).alias("is_skipped_label_played_at_month_avg_ms_played"),
        F.max("ms_played").over(w_source_by_played_at_moving_2).alias("ms_played_max_3"),
        F.sum("ms_played").over(w_all_by_played_at_running).alias("running_ms_played"),
        F.first("ms_played", ignorenulls=True).over(w_source_by_played_at_running).alias("first_known_ms_played"),
        F.row_number().over(w_all_by_played_at).alias("seq_no"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped_label_played_at_m), 2).alias("ms_played_vs_is_skipped_label_played_at_month_av"),
        F.last("played_at").over(w_all_by_played_at_full).alias("last_played_at"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "played_at",
        "source",
        "played_at_cume_dist",
        "is_skipped_label_played_at_month_avg_ms_played",
        "ms_played_max_3",
        "running_ms_played",
        "first_known_ms_played",
        "seq_no",
        "ms_played_vs_is_skipped_label_played_at_month_av",
        "last_played_at",
        F.min("ms_played_max_3").over(w_played_at_to_date_last_playe).alias("played_at_to_date_last_played_at_year_min_ms_pla"),
        F.lead("running_ms_played", 2).over(w_all_by_is_skipped_label_play).alias("next2_running_ms_played"),
        F.max("running_ms_played").over(w_played_at_to_date_last_playe).alias("played_at_to_date_last_played_at_year_max_runnin"),
        F.round(F.cume_dist().over(w_all_by_is_skipped_label_play), 3).alias("is_skipped_label_played_at_month_avg_ms_played_c"),
    )
)

totals = (
    with_windows_2
    .groupBy(
        F.date_trunc("month", "last_played_at").alias("last_played_at_month"),
        "seq_no",
        "source",
    )
    .agg(
        F.sum("ms_played_vs_is_skipped_label_played_at_month_av").alias("total_ms_played_vs_is_skipped_label_played_at_mo"),
        F.round(F.avg("ms_played_max_3"), 2).alias("avg_ms_played_max_3"),
        F.count(F.when(F.col("source") == "radio", 1)).alias("n_source_radio"),
        F.count("*").alias("n_rows"),
        F.countDistinct("source").alias("distinct_sources"),
        F.sum("first_known_ms_played").alias("total_first_known_ms_played"),
        F.avg("running_ms_played").alias("avg_running_ms_played_check"),
    )
    .filter(F.col("avg_running_ms_played_check") > 404500)
    .drop("avg_running_ms_played_check")
)

result = (
    totals
    .select(
        "last_played_at_month",
        "source",
        "total_ms_played_vs_is_skipped_label_played_at_mo",
        "avg_ms_played_max_3",
        "n_source_radio",
        "n_rows",
        "total_first_known_ms_played",
    )
    .orderBy(F.col("total_ms_played_vs_is_skipped_label_played_at_mo").desc())
)
