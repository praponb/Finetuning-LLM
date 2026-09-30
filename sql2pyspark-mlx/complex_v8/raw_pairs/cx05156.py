from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_by_played_at_full = Window.partitionBy("track_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_ties = Window.orderBy(F.col("played_at").desc_nulls_first())
w_track_id_by_played_at = Window.partitionBy("track_id").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_is_skipped_by_played_at = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc(), "id")
w_is_skipped_by_played_at_ties = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc())
w_is_skipped_by_played_at_runn = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_full = Window.orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at_running = Window.orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_by_played_at_full = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_played_at_full = Window.orderBy(F.col("first_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_is_skipped_by_la = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("last_played_at_2").desc(), "id")
w_track_id = Window.partitionBy("track_id")
w_listener_id_is_skipped_by_la_2 = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("last_played_at_2").desc(), "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(6, 10))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.plan").alias("listener_plan"),
    )
)

sequenced = (
    with_parent
    .select(
        "*",
        F.first("played_at").over(w_track_id_by_played_at_full).alias("first_played_at"),
        F.round(F.percent_rank().over(w_all_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.row_number().over(w_track_id_by_played_at).alias("rn"),
        F.ntile(10).over(w_is_skipped_by_played_at).alias("played_at_decile"),
        F.nth_value("played_at", 2).over(w_track_id_by_played_at_full).alias("second_played_at"),
        F.dense_rank().over(w_is_skipped_by_played_at_ties).alias("played_at_dense_rank"),
        F.first("played_at", ignorenulls=True).over(w_is_skipped_by_played_at_runn).alias("first_known_played_at"),
        F.round(F.percent_rank().over(w_is_skipped_by_played_at_ties), 3).alias("played_at_percent_rank_2"),
        F.last("played_at").over(w_all_by_played_at_full).alias("last_played_at"),
        F.first("played_at", ignorenulls=True).over(w_all_by_played_at_running).alias("first_known_played_at_2"),
        F.last("played_at").over(w_is_skipped_by_played_at_full).alias("last_played_at_2"),
        F.round(F.cume_dist().over(w_is_skipped_by_played_at), 3).alias("played_at_cume_dist"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.first("played_at_cume_dist").over(w_all_by_first_played_at_full).alias("first_played_at_cume_dist"),
        F.row_number().over(w_listener_id_is_skipped_by_la).alias("seq_no"),
        F.sum("played_at_percent_rank_2").over(w_track_id).alias("track_id_sum_played_at_percent_rank_2"),
        F.round(F.col("played_at_decile") / F.nullif(F.sum("played_at_decile").over(w_track_id), F.lit(0)), 4).alias("played_at_decile_share_of_track_id"),
        F.sum("played_at_dense_rank").over(w_track_id).alias("track_id_sum_played_at_dense_rank"),
        F.sum("rn").over(w_listener_id_is_skipped_by_la_2).alias("rn_sum_7"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "listener_id",
        "played_at",
        "listener_interests",
        "listener_plan",
        "first_played_at",
        "played_at_percent_rank",
        "played_at_decile",
        "second_played_at",
        "first_known_played_at",
        "played_at_percent_rank_2",
        "last_played_at",
        "first_known_played_at_2",
        "last_played_at_2",
        "played_at_cume_dist",
        "first_played_at_cume_dist",
        "seq_no",
        "track_id_sum_played_at_percent_rank_2",
        "played_at_decile_share_of_track_id",
        "track_id_sum_played_at_dense_rank",
        "rn_sum_7",
        "rows_in_track_id",
        F.round(F.col("played_at_percent_rank") * 1.07, 2).alias("played_at_percent_rank_adj"),
    )
    .orderBy(F.col("listener_plan").desc())
)
