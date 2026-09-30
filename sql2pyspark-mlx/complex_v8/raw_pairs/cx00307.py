from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_played_at_month_by_played_at = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("played_at", "id")
w_played_at_month_by_played_at_2 = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_month_played_at_se = Window.partitionBy(F.month("played_at"), "played_at_seq_2").orderBy(F.col("played_at_seq_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_seq_2 = Window.partitionBy("played_at_seq_2")
w_played_at_month_played_at_se_2 = Window.partitionBy(F.month("played_at"), "played_at_seq_2").orderBy(F.col("played_at_seq_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_played_at_to_date_by = Window.partitionBy(F.to_date("second_played_at")).orderBy(F.col("played_at_seq").desc(), "id")

valid = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("source") != "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

ranked = (
    valid
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        F.row_number().over(w_all_by_played_at).alias("played_at_seq"),
        F.ntile(3).over(w_played_at_month_by_played_at).alias("played_at_tercile"),
        F.nth_value("played_at", 2).over(w_played_at_month_by_played_at_2).alias("second_played_at"),
        F.row_number().over(w_played_at_month_by_played_at).alias("played_at_seq_2"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "played_at_seq",
        "second_played_at",
        "played_at_seq_2",
        F.nth_value("played_at", 2).over(w_played_at_month_played_at_se).alias("second_played_at_2"),
        F.max("played_at_seq").over(w_played_at_seq_2).alias("played_at_seq_2_max_played_at_seq"),
        F.first("second_played_at").over(w_played_at_month_played_at_se).alias("first_second_played_at"),
        F.sum("played_at_tercile").over(w_played_at_month_played_at_se_2).alias("running_played_at_tercile"),
        F.row_number().over(w_second_played_at_to_date_by).alias("seq_no"),
    )
)

detail_rows = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.device") == "desktop"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.second_played_at",
        "s.second_played_at_2",
        "s.first_second_played_at",
        "s.running_played_at_tercile",
        "s.seq_no",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.device").alias("play_device"),
    )
)

result = (
    detail_rows
    .rollup("listener_id", "seq_no")
    .agg(
        F.grouping("listener_id").alias("is_total_listener_id"),
        F.round(F.stddev("running_played_at_tercile"), 2).alias("stddev_running_played_at_tercile"),
    )
)
