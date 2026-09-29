from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_played_at = Window.partitionBy("source").orderBy(F.col("played_at").desc(), "id")
w_listener_id_source = Window.partitionBy("listener_id", "source")
w_source_by_played_at_prior_3 = Window.partitionBy("source").orderBy("played_at", "id").rowsBetween(-3, -1)
w_is_skipped_by_ms_played_movi = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-2, Window.currentRow)
w_listener_id_played_at_to_dat = Window.partitionBy("listener_id", F.to_date("played_at"))
w_track_id_source = Window.partitionBy("track_id", "source")
w_source_played_at_to_date_by = Window.partitionBy("source", F.to_date("played_at")).orderBy("played_at", "id")
w_listener_id_by_played_at_ful = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc(), "id")
w_played_at_month_by_played_at = Window.partitionBy(F.month("played_at")).orderBy("played_at", "id")
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy("played_at", "id")

valid_plays = (
    df_plays
    .filter((F.col("device").isNotNull()) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

windowed = (
    valid_plays
    .select(
        "id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.round(F.cume_dist().over(w_source_by_played_at), 3).alias("played_at_cume_dist"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id_source), 2).alias("ms_played_vs_listener_id_source_avg"),
        F.max("ms_played").over(w_source_by_played_at_prior_3).alias("ms_played_max_3_prior"),
        F.size(F.collect_set("track_id").over(w_listener_id_source)).alias("distinct_track_id_in_listener_id_source"),
        F.sum("ms_played").over(w_is_skipped_by_ms_played_movi).alias("ms_played_sum_3"),
        F.count(F.lit(1)).over(w_listener_id_played_at_to_dat).alias("rows_in_listener_id_played_at_to_date"),
        F.sum("ms_played").over(w_track_id_source).alias("track_id_source_sum_ms_played"),
        F.row_number().over(w_source_played_at_to_date_by).alias("played_at_seq"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id_played_at_to_dat), 2).alias("ms_played_vs_listener_id_played_at_to_date_avg"),
        F.first("ms_played").over(w_listener_id_by_played_at_ful).alias("first_ms_played"),
        F.round(F.cume_dist().over(w_source_played_at_to_date_by), 3).alias("played_at_cume_dist_2"),
        F.row_number().over(w_is_skipped_by_ms_played).alias("ms_played_seq"),
        F.nth_value("ms_played", 2).over(w_listener_id_by_played_at_ful).alias("second_ms_played"),
        F.row_number().over(w_played_at_month_by_played_at).alias("seq_no"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_source_played_at_to_date_by))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.row_number().over(w_listener_id_by_played_at).alias("played_at_seq_2"),
    )
)

top_per_group = windowed.filter((F.col("played_at_seq") <= 5) & (F.col("source") == "playlist"))

result = (
    top_per_group
    .select(
        "id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "ms_played_vs_listener_id_source_avg",
        "ms_played_max_3_prior",
        "distinct_track_id_in_listener_id_source",
        "ms_played_sum_3",
        "rows_in_listener_id_played_at_to_date",
        "track_id_source_sum_ms_played",
        "played_at_seq",
        "ms_played_vs_listener_id_played_at_to_date_avg",
        "first_ms_played",
        "played_at_cume_dist_2",
        "ms_played_seq",
        "second_ms_played",
        "seq_no",
        "minutes_since_prev_played_at",
        "played_at_seq_2",
        F.concat(F.col("source"), F.lit("#"), F.col("first_ms_played").cast("string")).alias("source_ref"),
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
    )
    .orderBy(F.col("rows_in_listener_id_played_at_to_date").asc())
)
