from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_flag_listener_id = Window.partitionBy("is_skipped_flag", "listener_id").orderBy("is_skipped_flag", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_listener_id_by_play = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc(), "id")
w_device_played_at_month_by_pl = Window.partitionBy("device", F.date_trunc("month", "played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_flag_listener_id_2 = Window.partitionBy("is_skipped_flag", "listener_id").orderBy("is_skipped_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_is_skipped = Window.partitionBy("track_id", "is_skipped")
w_is_skipped_track_id_by_playe = Window.partitionBy("is_skipped", "track_id").orderBy("played_at", "id")
w_track_id_listener_id_by_play_2 = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc())
w_is_skipped_flag_listener_id_3 = Window.partitionBy("is_skipped_flag", "listener_id").orderBy("is_skipped_flag", "id")
w_device_played_at_month_by_pl_2 = Window.partitionBy("device", F.date_trunc("month", "played_at")).orderBy("played_at", "id")
w_listener_id = Window.partitionBy("listener_id")

filtered = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.nth_value("played_at", 2).over(w_is_skipped_flag_listener_id).alias("second_played_at"),
        F.lag("played_at").over(w_track_id_listener_id_by_play).alias("prev_played_at"),
        F.nth_value("played_at", 2).over(w_device_played_at_month_by_pl).alias("second_played_at_2"),
        F.max("is_skipped_flag").over(w_is_skipped_flag_listener_id_2).alias("max_is_skipped_flag_so_far"),
        F.round(F.col("is_skipped_flag") / F.nullif(F.sum("is_skipped_flag").over(w_track_id_is_skipped), F.lit(0)), 4).alias("is_skipped_flag_share_of_track_id_is_skipped"),
        (F.col("is_skipped_flag") - F.lag("is_skipped_flag").over(w_is_skipped_track_id_by_playe)).alias("is_skipped_flag_change"),
        F.rank().over(w_track_id_listener_id_by_play_2).alias("played_at_rank"),
        F.ntile(4).over(w_is_skipped_flag_listener_id_3).alias("is_skipped_flag_quartile"),
        F.lead("is_skipped_flag").over(w_device_played_at_month_by_pl_2).alias("next_is_skipped_flag"),
        F.row_number().over(w_is_skipped_track_id_by_playe).alias("played_at_seq"),
        F.round(F.col("is_skipped_flag") / F.nullif(F.sum("is_skipped_flag").over(w_listener_id), F.lit(0)), 4).alias("is_skipped_flag_share_of_listener_id"),
        F.round(F.avg("is_skipped_flag").over(w_track_id_is_skipped), 2).alias("track_id_is_skipped_avg_is_skipped_flag"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_track_id_listener_id_by_play))) / 60, 1).alias("minutes_since_prev_played_at"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "is_skipped_flag",
        "second_played_at",
        "is_skipped_flag_share_of_track_id_is_skipped",
        "is_skipped_flag_change",
        "played_at_rank",
        "is_skipped_flag_quartile",
        "next_is_skipped_flag",
        "is_skipped_flag_share_of_listener_id",
        "track_id_is_skipped_avg_is_skipped_flag",
        "minutes_since_prev_played_at",
    )
    .orderBy(F.col("is_skipped_flag").desc())
)
