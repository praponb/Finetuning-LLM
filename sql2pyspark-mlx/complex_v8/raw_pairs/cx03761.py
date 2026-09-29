from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_to_date_listener_p = Window.partitionBy(F.to_date("played_at"), "listener_plan").orderBy("track_bpm")
w_played_at_to_date = Window.partitionBy(F.to_date("played_at"))
w_is_skipped_listener_id_by_pl = Window.partitionBy("is_skipped", "listener_id").orderBy("played_at")
w_is_skipped_listener_id_by_pl_2 = Window.partitionBy("is_skipped", "listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_track_bpm = Window.partitionBy("track_id").orderBy(F.col("track_bpm").desc(), "id")
w_track_id_is_skipped_by_playe = Window.partitionBy("track_id", "is_skipped").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_listener_id_by_pl_3 = Window.partitionBy("is_skipped", "listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
    )
)

joined = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.plan").alias("listener_plan"),
        F.col("t.title").alias("track_title"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

with_trends = (
    joined
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "listener_interests",
        "track_title",
        "track_bpm",
        F.dense_rank().over(w_played_at_to_date_listener_p).alias("track_bpm_dense_rank"),
        F.round(F.avg("track_bpm").over(w_played_at_to_date), 2).alias("played_at_to_date_avg_track_bpm"),
        F.round(F.percent_rank().over(w_is_skipped_listener_id_by_pl), 3).alias("played_at_percent_rank"),
        F.count(F.lit(1)).over(w_played_at_to_date).alias("rows_in_played_at_to_date"),
        F.first("played_at", ignorenulls=True).over(w_is_skipped_listener_id_by_pl_2).alias("first_known_played_at"),
        F.sum("track_bpm").over(w_played_at_to_date).alias("played_at_to_date_sum_track_bpm"),
        F.lead("track_bpm").over(w_track_id_by_track_bpm).alias("next_track_bpm"),
        F.last("track_bpm", ignorenulls=True).over(w_track_id_is_skipped_by_playe).alias("last_known_track_bpm"),
        F.nth_value("played_at", 2).over(w_is_skipped_listener_id_by_pl_3).alias("second_played_at"),
    )
)

result = (
    with_trends
    .filter(F.month(F.col("played_at")).isin(1, 3))
    .select(
        "id",
        "track_id",
        "is_skipped",
        "listener_interests",
        "track_title",
        "track_bpm",
        "track_bpm_dense_rank",
        "played_at_to_date_avg_track_bpm",
        "rows_in_played_at_to_date",
        "first_known_played_at",
        "played_at_to_date_sum_track_bpm",
        "next_track_bpm",
        "last_known_track_bpm",
        "second_played_at",
    )
)
