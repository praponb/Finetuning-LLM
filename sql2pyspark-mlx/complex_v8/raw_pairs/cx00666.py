from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_played_at_ties = Window.partitionBy("source").orderBy("played_at")
w_source_by_played_at = Window.partitionBy("source").orderBy("played_at", "id")
w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_played_at_rank_played_at_ran = Window.partitionBy("played_at_rank", "played_at_rank_2_bucket").orderBy(F.col("ms_played").desc(), "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_played_at_quintile_played_at = Window.partitionBy("played_at_quintile", F.date_trunc("month", "played_at"))
w_played_at_rank_by_distinct_d = Window.partitionBy("played_at_rank").orderBy(F.col("distinct_device_in_track_id_is_skipped").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_quintile_is_skippe = Window.partitionBy("played_at_quintile", "is_skipped").orderBy("played_at_quartile", "id")
w_played_at_rank_played_at_ran_2 = Window.partitionBy("played_at_rank", "played_at_rank_2_bucket").orderBy(F.col("ms_played").desc())

valid_plays = (
    df_plays
    .filter((F.col("source").isNotNull()) | (F.month(F.col("played_at")).isin(1, 3)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws(": ", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

windowed = (
    valid_plays
    .select(
        "id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source_device_label",
        F.rank().over(w_source_by_played_at_ties).alias("played_at_rank"),
        F.lag("played_at", 2).over(w_source_by_played_at).alias("prev2_played_at"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_is_skipped), 2).alias("ms_played_vs_track_id_is_skipped_avg"),
        F.ntile(5).over(w_all_by_played_at).alias("played_at_quintile"),
        F.size(F.collect_set("device").over(w_track_id_is_skipped)).alias("distinct_device_in_track_id_is_skipped"),
        F.ntile(4).over(w_source_by_played_at).alias("played_at_quartile"),
        F.size(F.collect_set("listener_id").over(w_track_id_is_skipped)).alias("distinct_listener_id_in_track_id_is_skipped"),
        F.rank().over(w_source_by_played_at).alias("played_at_rank_2"),
    )
)

derived = (
    windowed
    .select(
        "*",
        (F.floor(F.col("played_at_quintile") / 2) * 2).alias("played_at_quintile_bucket"),
        (F.floor(F.col("played_at_rank_2") / 2) * 2).alias("played_at_rank_2_bucket"),
        F.least(F.col("played_at_quintile"), F.col("played_at_rank_2")).alias("least_played_at_quintile_played_at_rank_2"),
    )
)

with_trends = (
    derived
    .select(
        "*",
        F.round(F.cume_dist().over(w_played_at_rank_played_at_ran), 3).alias("ms_played_cume_dist"),
        F.min("played_at_quintile").over(w_is_skipped).alias("is_skipped_min_played_at_quintile"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.nth_value("played_at", 2).over(w_played_at_rank_by_distinct_d).alias("second_played_at"),
        F.dense_rank().over(w_played_at_rank_played_at_ran).alias("ms_played_dense_rank"),
        F.lead("prev2_played_at", 3).over(w_played_at_quintile_is_skippe).alias("next3_prev2_played_at"),
        F.round(F.percent_rank().over(w_played_at_rank_played_at_ran_2), 3).alias("ms_played_percent_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("played_at_quartile").between(2, 7))
    .select(
        "id",
        "played_at",
        "ms_played",
        "played_at_rank",
        "prev2_played_at",
        "ms_played_vs_track_id_is_skipped_avg",
        "distinct_device_in_track_id_is_skipped",
        "played_at_quartile",
        "distinct_listener_id_in_track_id_is_skipped",
        "played_at_rank_2",
        "played_at_quintile_bucket",
        "played_at_rank_2_bucket",
        "is_skipped_min_played_at_quintile",
        "rows_in_is_skipped",
        "second_played_at",
        "next3_prev2_played_at",
        "ms_played_percent_rank",
    )
    .orderBy(F.col("played_at_rank_2").asc_nulls_last(), "id")
)
