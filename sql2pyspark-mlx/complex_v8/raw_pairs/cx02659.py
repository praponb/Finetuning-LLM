from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_device_by_played = Window.partitionBy("is_skipped", "device").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_source_by_played_at = Window.partitionBy("source").orderBy("played_at", "id")
w_track_id_listener_id_by_play = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc(), "id")
w_listener_id_device_by_played = Window.partitionBy("listener_id", "device").orderBy("played_at", "id")
w_all_by_played_at_percent_ran = Window.orderBy("played_at_percent_rank", "id").rowsBetween(-2, -1)
w_track_id_device_by_played_at = Window.partitionBy("track_id", "device").orderBy(F.col("played_at_tercile").desc(), "id").rowsBetween(-2, -1)
w_played_at_rank_2_prev3_playe = Window.partitionBy("played_at_rank_2", F.to_date("prev3_played_at"))
w_played_at_month_by_played_at = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy(F.col("played_at_percent_rank").desc(), "id")
w_all_by_played_at = Window.orderBy("played_at", "id")

recent = (
    df_plays
    .filter(F.col("source") == "album")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

metrics = (
    recent
    .select(
        "*",
        F.round(F.percent_rank().over(w_is_skipped_device_by_played), 3).alias("played_at_percent_rank"),
        F.lag("played_at", 3).over(w_source_by_played_at).alias("prev3_played_at"),
        F.rank().over(w_track_id_listener_id_by_play).alias("played_at_rank"),
        F.ntile(5).over(w_source_by_played_at).alias("played_at_quintile"),
        F.rank().over(w_is_skipped_device_by_played).alias("played_at_rank_2"),
        F.lead("played_at").over(w_listener_id_device_by_played).alias("next_played_at"),
        F.row_number().over(w_track_id_listener_id_by_play).alias("played_at_seq"),
        F.row_number().over(w_source_by_played_at).alias("played_at_seq_2"),
        F.ntile(3).over(w_track_id_listener_id_by_play).alias("played_at_tercile"),
    )
)

sequenced = (
    metrics
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "played_at_percent_rank",
        "played_at_rank",
        "played_at_quintile",
        "played_at_rank_2",
        "next_played_at",
        "played_at_seq",
        "played_at_seq_2",
        "played_at_tercile",
        F.round(F.avg("played_at_rank_2").over(w_all_by_played_at_percent_ran), 2).alias("played_at_rank_2_avg_2_prior"),
        F.round(F.avg("played_at_tercile").over(w_track_id_device_by_played_at), 2).alias("played_at_tercile_avg_2_prior"),
        F.round(F.col("played_at_seq_2") - F.avg("played_at_seq_2").over(w_played_at_rank_2_prev3_playe), 2).alias("played_at_seq_2_vs_played_at_rank_2_prev3_played"),
        F.lag("played_at_quintile").over(w_played_at_month_by_played_at).alias("prev_played_at_quintile"),
        F.lag("played_at_rank").over(w_all_by_played_at).alias("prev_played_at_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "played_at_percent_rank",
        "played_at_rank",
        "played_at_quintile",
        "next_played_at",
        "played_at_seq_2",
        "played_at_rank_2_avg_2_prior",
        "played_at_tercile_avg_2_prior",
        "prev_played_at_quintile",
    )
    .orderBy("prev_played_at_quintile", "id")
    .limit(20)
)
