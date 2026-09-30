from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_played_at_to_date_listener_i = Window.partitionBy(F.to_date("played_at"), "listener_id").orderBy(F.col("played_at_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id = Window.partitionBy("track_id")
w_device_by_played_at_month = Window.partitionBy("device").orderBy(F.col("played_at_month").desc(), "id")
w_played_at_month_by_played_at = Window.partitionBy("played_at_month").orderBy("played_at", "id")
w_all_by_played_at_month_runni = Window.orderBy(F.col("played_at_month").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device = Window.partitionBy("device")
w_played_at_to_date_listener_i_2 = Window.partitionBy(F.to_date("played_at"), "listener_id").orderBy(F.col("played_at_month").desc())
w_listener_plan = Window.partitionBy("listener_plan")
w_played_at_month_tercile_by_s = Window.partitionBy("played_at_month_tercile").orderBy(F.col("second_played_at").desc(), "id")
w_listener_id_played_at_seq_by = Window.partitionBy("listener_id", "played_at_seq").orderBy("played_at_month_decile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.month(F.col("played_at")).alias("played_at_month"),
    )
)

combined_details_listener = (
    scoped.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.played_at_month",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
    )
)

with_trends = (
    combined_details_listener
    .select(
        "*",
        (F.col("played_at_month") - F.lag("played_at_month").over(w_all_by_played_at)).alias("played_at_month_change"),
        F.nth_value("played_at", 2).over(w_played_at_to_date_listener_i).alias("second_played_at"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.ntile(3).over(w_device_by_played_at_month).alias("played_at_month_tercile"),
        F.ntile(10).over(w_device_by_played_at_month).alias("played_at_month_decile"),
        F.sum("played_at_month").over(w_track_id).alias("track_id_sum_played_at_month"),
        F.row_number().over(w_played_at_month_by_played_at).alias("played_at_seq"),
        F.min("played_at_month").over(w_all_by_played_at_month_runni).alias("min_played_at_month_so_far"),
        F.round(F.col("played_at_month") / F.nullif(F.sum("played_at_month").over(w_device), F.lit(0)), 4).alias("played_at_month_share_of_device"),
        F.min("played_at_month").over(w_track_id).alias("track_id_min_played_at_month"),
        F.dense_rank().over(w_played_at_to_date_listener_i_2).alias("played_at_month_dense_rank"),
        F.min("played_at_month").over(w_listener_plan).alias("listener_plan_min_played_at_month"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "played_at",
        "device",
        "played_at_month",
        "listener_plan",
        "listener_interests",
        "listener_username",
        "played_at_month_change",
        "second_played_at",
        "rows_in_track_id",
        "track_id_sum_played_at_month",
        "played_at_seq",
        "min_played_at_month_so_far",
        "track_id_min_played_at_month",
        "played_at_month_dense_rank",
        F.row_number().over(w_played_at_month_tercile_by_s).alias("second_played_at_seq"),
        F.count(F.lit(1)).over(w_device).alias("rows_in_device"),
        F.first("played_at_month_tercile", ignorenulls=True).over(w_listener_id_played_at_seq_by).alias("first_known_played_at_month_tercile"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "device",
        "played_at_month",
        "listener_plan",
        "played_at_month_change",
        "second_played_at",
        "rows_in_track_id",
        "played_at_seq",
        "min_played_at_month_so_far",
        "played_at_month_dense_rank",
        "second_played_at_seq",
        "rows_in_device",
        (F.col("second_played_at_seq") + F.col("played_at_month_change")).alias("second_played_at_seq_plus_played_at_month_change"),
        F.when(F.col("played_at_month_change") >= 6, "A").when(F.col("played_at_month_change") >= -4, "B").otherwise("C").alias("played_at_month_change_tier"),
    )
    .orderBy(F.col("rows_in_device").desc_nulls_first())
)
