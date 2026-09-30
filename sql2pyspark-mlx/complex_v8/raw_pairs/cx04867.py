from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at = Window.partitionBy("track_id").orderBy(F.col("played_at").desc(), "id")
w_track_id_by_played_at_full = Window.partitionBy("track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_listener_id_by_play = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc(), "id")
w_track_id_listener_id_by_play_2 = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_listener_id_by_play_3 = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc())
w_track_id_second_played_at_ye = Window.partitionBy("track_id", F.year("second_played_at"))
w_all_by_minutes_since_prev_pl = Window.orderBy(F.col("minutes_since_prev_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_second_played_at = Window.orderBy("second_played_at", "id")
w_track_id = Window.partitionBy("track_id")
w_second_played_at_year_source = Window.partitionBy(F.year("second_played_at"), "source").orderBy(F.col("last_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_plays = (
    df_plays
    .filter((F.col("device").isNotNull()) & (F.col("track_id") >= 23))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

sequenced = (
    valid_plays
    .select(
        "*",
        F.row_number().over(w_track_id_by_played_at).alias("played_at_seq"),
        F.last("played_at").over(w_track_id_by_played_at_full).alias("last_played_at"),
        F.lead("played_at", 2).over(w_track_id_listener_id_by_play).alias("next2_played_at"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_track_id_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.nth_value("played_at", 2).over(w_track_id_listener_id_by_play_2).alias("second_played_at"),
        F.round(F.percent_rank().over(w_track_id_listener_id_by_play_3), 3).alias("played_at_percent_rank"),
    )
)

with_activity = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") >= 151112) & (F.col("p.track_id").isNotNull()), "left_semi")
)

windowed = (
    with_activity
    .select(
        "*",
        F.size(F.collect_set("listener_id").over(w_track_id_second_played_at_ye)).alias("distinct_listener_id_in_track_id_second_played_a"),
        F.round(F.col("played_at_seq") - F.avg("played_at_seq").over(w_track_id_second_played_at_ye), 2).alias("played_at_seq_vs_track_id_second_played_at_year_"),
        F.round(F.col("played_at_percent_rank") / F.nullif(F.sum("played_at_percent_rank").over(w_track_id_second_played_at_ye), F.lit(0)), 4).alias("played_at_percent_rank_share_of_track_id_second_"),
        F.last("next2_played_at", ignorenulls=True).over(w_all_by_minutes_since_prev_pl).alias("last_known_next2_played_at"),
        F.round((F.unix_timestamp("last_played_at") - F.unix_timestamp(F.lag("last_played_at").over(w_all_by_second_played_at))) / 60, 1).alias("minutes_since_prev_last_played_at"),
    )
)

qualified = (
    windowed.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (~F.col("p.device").isin("phone", "tv")) & (F.col("p.ms_played") <= 241585), "left_anti")
)

metrics = (
    qualified
    .select(
        "*",
        F.size(F.collect_set("device").over(w_track_id)).alias("distinct_device_in_track_id"),
        F.sum("played_at_seq").over(w_track_id).alias("track_id_sum_played_at_seq"),
        F.max("played_at_percent_rank").over(w_second_played_at_year_source).alias("max_played_at_percent_rank_so_far"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
    )
)

result = (
    metrics
    .groupBy("source", "played_at_seq")
    .pivot("device", ["car", "phone", "desktop"])
    .agg(F.count("distinct_listener_id_in_track_id_second_played_a"))
)
