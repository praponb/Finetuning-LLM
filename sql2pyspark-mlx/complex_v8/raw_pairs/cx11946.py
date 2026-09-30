from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_played_at = Window.partitionBy("device").orderBy("played_at", "id")
w_track_id_is_skipped_by_playe = Window.partitionBy("track_id", "is_skipped").orderBy("played_at")
w_track_id_is_skipped_by_playe_2 = Window.partitionBy("track_id", "is_skipped").orderBy("played_at", "id")
w_played_at_to_date_device_by = Window.partitionBy(F.to_date("played_at"), "device").orderBy("played_at", "id")
w_device_by_played_at_full = Window.partitionBy("device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_to_date_device_by_2 = Window.partitionBy(F.to_date("played_at"), "device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_ties = Window.orderBy(F.col("played_at").desc())
w_prev_played_at_2_month = Window.partitionBy(F.date_trunc("month", "prev_played_at_2"))
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_all_by_prev_played_at_2_movi = Window.orderBy(F.col("prev_played_at_2").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)

base = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
    )
)

windowed = (
    base
    .select(
        "*",
        F.round(F.cume_dist().over(w_device_by_played_at), 3).alias("played_at_cume_dist"),
        F.dense_rank().over(w_track_id_is_skipped_by_playe).alias("played_at_dense_rank"),
        F.rank().over(w_device_by_played_at).alias("played_at_rank"),
        F.ntile(3).over(w_track_id_is_skipped_by_playe_2).alias("played_at_tercile"),
        F.lag("played_at", 2).over(w_track_id_is_skipped_by_playe_2).alias("prev2_played_at"),
        F.rank().over(w_played_at_to_date_device_by).alias("played_at_rank_2"),
        F.nth_value("played_at", 2).over(w_device_by_played_at_full).alias("second_played_at"),
        F.round(F.cume_dist().over(w_track_id_is_skipped_by_playe), 3).alias("played_at_cume_dist_2"),
        F.round(F.percent_rank().over(w_device_by_played_at), 3).alias("played_at_percent_rank"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_played_at_to_date_device_by))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.first("played_at", ignorenulls=True).over(w_played_at_to_date_device_by_2).alias("first_known_played_at"),
        F.lag("played_at").over(w_played_at_to_date_device_by).alias("prev_played_at"),
        F.lag("played_at").over(w_device_by_played_at).alias("prev_played_at_2"),
        F.ntile(10).over(w_track_id_is_skipped_by_playe_2).alias("played_at_decile"),
    )
)

first_rows = (
    windowed
    .filter(F.col("played_at_dense_rank") <= 3)
    .drop("played_at_dense_rank")
)

with_trends = (
    first_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source_clean",
        "played_at_cume_dist",
        "played_at_rank",
        "played_at_tercile",
        "prev2_played_at",
        "played_at_rank_2",
        "played_at_cume_dist_2",
        "played_at_percent_rank",
        "minutes_since_prev_played_at",
        "prev_played_at",
        "prev_played_at_2",
        "played_at_decile",
        F.round(F.cume_dist().over(w_all_by_played_at_ties), 3).alias("played_at_cume_dist_3"),
        F.size(F.collect_set("track_id").over(w_prev_played_at_2_month)).alias("distinct_track_id_in_prev_played_at_2_month"),
        F.lag("played_at_rank").over(w_all_by_played_at).alias("prev_played_at_rank"),
        F.max("played_at_cume_dist").over(w_all_by_prev_played_at_2_movi).alias("played_at_cume_dist_max_3"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "device",
        "source_clean",
        "played_at_tercile",
        "prev2_played_at",
        "played_at_rank_2",
        "played_at_cume_dist_2",
        "played_at_percent_rank",
        "minutes_since_prev_played_at",
        "prev_played_at",
        "prev_played_at_2",
        "played_at_decile",
        "played_at_cume_dist_3",
        "distinct_track_id_in_prev_played_at_2_month",
        "prev_played_at_rank",
        "played_at_cume_dist_max_3",
        F.concat_ws(": ", F.col("source_clean"), F.col("device")).alias("source_clean_device_label"),
        F.round(F.col("played_at_percent_rank") * 0.25, 2).alias("played_at_percent_rank_adj"),
    )
    .orderBy(F.col("played_at_percent_rank").desc(), "prev_played_at_2")
)
