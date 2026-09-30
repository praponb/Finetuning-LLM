from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_track_id_by_ms_p = Window.partitionBy("listener_id", "track_id").orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id")
w_listener_id_played_at_month = Window.partitionBy("listener_id", F.date_trunc("month", "played_at"))
w_track_id_by_ms_played_moving = Window.partitionBy("track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-2, Window.currentRow)
w_listener_id_track_id_by_play = Window.partitionBy("listener_id", "track_id").orderBy("played_at", "id")
w_listener_id_track_id_by_ms_p_2 = Window.partitionBy("listener_id", "track_id").orderBy(F.col("ms_played").desc(), "id")
w_played_at_year = Window.partitionBy(F.year("played_at"))
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_track_id_by_next3_played_at = Window.partitionBy("track_id").orderBy(F.col("next3_played_at").desc(), "id").rowsBetween(-6, Window.currentRow)

filtered_plays = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.month(F.col("played_at")).isin(2, 3)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
    )
)

with_trends = (
    filtered_plays
    .select(
        "*",
        F.lead("played_at", 3).over(w_listener_id_track_id_by_ms_p).alias("next3_played_at"),
        F.dense_rank().over(w_track_id_by_ms_played).alias("ms_played_dense_rank"),
        F.round(F.avg("ms_played").over(w_listener_id_played_at_month), 2).alias("listener_id_played_at_month_avg_ms_played"),
        F.sum("ms_played").over(w_track_id_by_ms_played_moving).alias("ms_played_sum_3"),
        F.lag("played_at").over(w_listener_id_track_id_by_play).alias("prev_played_at"),
        F.min("ms_played").over(w_listener_id_played_at_month).alias("listener_id_played_at_month_min_ms_played"),
        F.row_number().over(w_listener_id_track_id_by_ms_p_2).alias("seq_no"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.count(F.lit(1)).over(w_played_at_year).alias("rows_in_played_at_year"),
        F.ntile(10).over(w_all_by_played_at).alias("played_at_decile"),
        F.round(F.avg("ms_played_dense_rank").over(w_track_id_by_next3_played_at), 2).alias("ms_played_dense_rank_avg_7"),
    )
)

result = (
    ranked
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "played_at_label",
        "next3_played_at",
        "listener_id_played_at_month_avg_ms_played",
        "ms_played_sum_3",
        "prev_played_at",
        "rows_in_played_at_year",
        "played_at_decile",
        "ms_played_dense_rank_avg_7",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.least(F.col("seq_no"), F.col("ms_played")).alias("least_seq_no_ms_played"),
    )
    .orderBy(F.col("listener_id_played_at_month_avg_ms_played").desc(), "id")
    .limit(10)
)
