from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at_ties = Window.partitionBy("track_id").orderBy(F.col("played_at").asc_nulls_last())
w_device_is_skipped = Window.partitionBy("device", "is_skipped")
w_played_at_year_by_played_at = Window.partitionBy(F.year("played_at")).orderBy("played_at_quarter", "id")
w_track_id_by_played_at_moving = Window.partitionBy("track_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_listener_id_played_at_month = Window.partitionBy("listener_id", F.date_trunc("month", "played_at"))
w_track_id_by_played_at = Window.partitionBy("track_id").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_listener_id_played_at_year_b = Window.partitionBy("listener_id", F.year("played_at")).orderBy("played_at", "id")
w_played_at_year_by_played_at_2 = Window.partitionBy(F.year("played_at")).orderBy("played_at_quarter")
w_played_at_year_by_played_at_3 = Window.partitionBy(F.year("played_at")).orderBy("played_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_quarter_year_by_pl = Window.partitionBy(F.year("played_at_quarter")).orderBy("played_at_quarter")
w_played_at_quarter_rank = Window.partitionBy("played_at_quarter_rank")
w_distinct_track_id_in_listene = Window.partitionBy("distinct_track_id_in_listener_id_played_at_month", "distinct_listener_id_in_device_is_skipped").orderBy("listener_id_played_at_month_avg_ms_played", "id")
w_is_skipped_device_group_by_p = Window.partitionBy("is_skipped", "device_group").orderBy("played_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_distinct_track = Window.partitionBy("is_skipped").orderBy("distinct_track_id_in_listener_id_played_at_month", "id")
w_played_at_quarter_rank_track = Window.partitionBy("played_at_quarter_rank", "track_id")

valid_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
    )
)

ranked = (
    valid_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "played_at_quarter",
        F.round(F.cume_dist().over(w_track_id_by_played_at_ties), 3).alias("played_at_cume_dist"),
        F.size(F.collect_set("listener_id").over(w_device_is_skipped)).alias("distinct_listener_id_in_device_is_skipped"),
        F.lag("played_at", 3).over(w_played_at_year_by_played_at).alias("prev3_played_at"),
        F.round(F.avg("ms_played").over(w_track_id_by_played_at_moving), 2).alias("ms_played_avg_2"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id_played_at_month), 2).alias("ms_played_vs_listener_id_played_at_month_avg"),
        F.round((F.unix_timestamp("played_at_quarter") - F.unix_timestamp(F.lag("played_at_quarter").over(w_track_id_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at_quarter"),
        F.size(F.collect_set("track_id").over(w_listener_id_played_at_month)).alias("distinct_track_id_in_listener_id_played_at_month"),
        F.row_number().over(w_listener_id_played_at_year_b).alias("rn"),
        F.rank().over(w_played_at_year_by_played_at_2).alias("played_at_quarter_rank"),
        F.count(F.lit(1)).over(w_played_at_year_by_played_at_3).alias("running_count"),
        F.round(F.percent_rank().over(w_played_at_quarter_year_by_pl), 3).alias("played_at_quarter_percent_rank"),
        F.round(F.avg("ms_played").over(w_listener_id_played_at_month), 2).alias("listener_id_played_at_month_avg_ms_played"),
    )
)

scored = (
    ranked
    .filter(F.col("played_at_quarter_percent_rank") <= 0.6)
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "played_at_quarter",
        "distinct_listener_id_in_device_is_skipped",
        "prev3_played_at",
        "ms_played_avg_2",
        "ms_played_vs_listener_id_played_at_month_avg",
        "minutes_since_prev_played_at_quarter",
        "distinct_track_id_in_listener_id_played_at_month",
        "rn",
        "played_at_quarter_rank",
        "running_count",
        "played_at_quarter_percent_rank",
        "listener_id_played_at_month_avg_ms_played",
        F.when(F.col("device").isin("tv", "desktop"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.size(F.collect_set("listener_id").over(w_played_at_quarter_rank)).alias("distinct_listener_id_in_played_at_quarter_rank"),
        F.row_number().over(w_distinct_track_id_in_listene).alias("listener_id_played_at_month_avg_ms_played_seq"),
        F.last("rn").over(w_is_skipped_device_group_by_p).alias("last_rn"),
        F.ntile(3).over(w_is_skipped_by_distinct_track).alias("distinct_track_id_in_listener_id_played_at_month_2"),
        F.count(F.lit(1)).over(w_played_at_quarter_rank_track).alias("rows_in_played_at_quarter_rank_track_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "played_at_quarter",
        "distinct_listener_id_in_device_is_skipped",
        "prev3_played_at",
        "ms_played_avg_2",
        "ms_played_vs_listener_id_played_at_month_avg",
        "minutes_since_prev_played_at_quarter",
        "distinct_track_id_in_listener_id_played_at_month",
        "rn",
        "played_at_quarter_rank",
        "played_at_quarter_percent_rank",
        "listener_id_played_at_month_avg_ms_played",
        "device_group",
        "distinct_listener_id_in_played_at_quarter_rank",
        "listener_id_played_at_month_avg_ms_played_seq",
        "distinct_track_id_in_listener_id_played_at_month_2",
        "rows_in_played_at_quarter_rank_track_id",
        F.date_format(F.col("played_at_quarter"), "yyyy-MM-dd").alias("played_at_quarter_label"),
        F.lower(F.trim(F.col("device_group"))).alias("device_group_clean"),
    )
    .orderBy(
        F.col("prev3_played_at").desc(),
        F.col("listener_id_played_at_month_avg_ms_played_seq").asc(),
    )
)
