from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_to_date_listener_i = Window.partitionBy(F.to_date("played_at"), "listener_id").orderBy("played_at")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_played_at_to_date_track_id_b = Window.partitionBy(F.to_date("played_at"), "track_id").orderBy("played_at_month", "id").rowsBetween(-2, -1)
w_track_id = Window.partitionBy("track_id")
w_played_at_to_date_listener_i_2 = Window.partitionBy(F.to_date("played_at"), "listener_id").orderBy("played_at", "id").rowsBetween(-1, Window.currentRow)
w_all_by_played_at_2 = Window.orderBy(F.col("played_at").desc(), "id")
w_played_at_to_date_track_id_b_2 = Window.partitionBy(F.to_date("played_at"), "track_id").orderBy("played_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_source = Window.partitionBy("listener_id", "source")
w_listener_id_first_known_play = Window.partitionBy("listener_id", F.month("first_known_played_at_month")).orderBy(F.col("played_at_percent_rank").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_track_id_min_ms_playe = Window.orderBy("track_id_min_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_known_played_at_month = Window.partitionBy(F.year("first_known_played_at_month"), "played_at_dense_rank")
w_all_by_track_id_min_ms_playe_2 = Window.orderBy("track_id_min_ms_played", "id")

scoped_plays = (
    df_plays
    .filter(F.col("ms_played") <= 223828)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
    )
)

ranked = (
    scoped_plays
    .select(
        "*",
        F.round(F.cume_dist().over(w_played_at_to_date_listener_i), 3).alias("played_at_cume_dist"),
        F.dense_rank().over(w_all_by_played_at).alias("played_at_dense_rank"),
        F.round(F.avg("ms_played").over(w_played_at_to_date_track_id_b), 2).alias("ms_played_avg_2_prior"),
        F.min("ms_played").over(w_track_id).alias("track_id_min_ms_played"),
        F.sum("ms_played").over(w_played_at_to_date_listener_i_2).alias("ms_played_sum_2"),
        F.round(F.percent_rank().over(w_all_by_played_at_2), 3).alias("played_at_percent_rank"),
        F.first("played_at_month", ignorenulls=True).over(w_played_at_to_date_track_id_b_2).alias("first_known_played_at_month"),
        F.sum("ms_played").over(w_listener_id_source).alias("listener_id_source_sum_ms_played"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.first("played_at_dense_rank").over(w_listener_id_first_known_play).alias("first_played_at_dense_rank"),
        F.first("ms_played_sum_2", ignorenulls=True).over(w_all_by_track_id_min_ms_playe).alias("first_known_ms_played_sum_2"),
        F.round(F.col("ms_played_sum_2") - F.avg("ms_played_sum_2").over(w_first_known_played_at_month), 2).alias("ms_played_sum_2_vs_first_known_played_at_month_y"),
        (F.col("played_at_dense_rank") - F.lag("played_at_dense_rank").over(w_all_by_track_id_min_ms_playe_2)).alias("played_at_dense_rank_change"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "track_id",
        "ms_played",
        "source",
        "played_at_month",
        "played_at_cume_dist",
        "ms_played_avg_2_prior",
        "track_id_min_ms_played",
        "played_at_percent_rank",
        "listener_id_source_sum_ms_played",
        "first_played_at_dense_rank",
        "ms_played_sum_2_vs_first_known_played_at_month_y",
        (F.col("track_id_min_ms_played") - F.col("ms_played")).alias("track_id_min_ms_played_minus_ms_played"),
        F.round((F.unix_timestamp(F.col("played_at")) - F.unix_timestamp(F.col("played_at_month"))) / 3600, 1).alias("hours_played_at_month_to_played_at"),
    )
    .orderBy(F.col("first_played_at_dense_rank").desc())
)
