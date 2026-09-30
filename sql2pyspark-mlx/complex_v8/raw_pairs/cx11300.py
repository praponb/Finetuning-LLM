from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_ties = Window.orderBy(F.col("ms_played").asc_nulls_last())
w_listener_id_by_ms_played_mov = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(-2, Window.currentRow)
w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy("ms_played", "id")
w_all_by_listener_signup_date = Window.orderBy("listener_signup_date")
w_all_by_listener_signup_date_2 = Window.orderBy("listener_signup_date", "id").rowsBetween(-3, -1)
w_all_by_listener_signup_date_3 = Window.orderBy("listener_signup_date", "id").rowsBetween(-6, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")
w_listener_id_by_ms_played_mov_2 = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(-6, Window.currentRow)

recent_plays = (
    df_plays
    .filter((F.col("track_id") >= 12) & (F.col("ms_played") <= 214343))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

combined_details = (
    recent_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username"),
    )
)

metrics = (
    combined_details
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.dense_rank().over(w_all_by_ms_played_ties).alias("ms_played_dense_rank"),
        F.round(F.avg("listener_birth_year").over(w_listener_id_by_ms_played_mov), 2).alias("listener_birth_year_avg_3"),
        F.lag("ms_played").over(w_track_id_by_ms_played).alias("prev_ms_played"),
        F.dense_rank().over(w_all_by_listener_signup_date).alias("listener_signup_date_dense_rank"),
        F.sum("ms_played").over(w_all_by_listener_signup_date_2).alias("ms_played_sum_3_prior"),
        F.round(F.percent_rank().over(w_all_by_listener_signup_date), 3).alias("listener_signup_date_percent_rank"),
        F.round(F.avg("listener_birth_year").over(w_all_by_listener_signup_date_3), 2).alias("listener_birth_year_avg_7"),
        F.size(F.collect_set("track_id").over(w_listener_id)).alias("distinct_track_id_in_listener_id"),
        F.round(F.avg("ms_played").over(w_listener_id_by_ms_played_mov_2), 2).alias("ms_played_avg_7"),
    )
)

labeled = (
    metrics
    .filter(F.col("ms_played") < 225733)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "ms_played_dense_rank",
        "listener_birth_year_avg_3",
        "prev_ms_played",
        "listener_signup_date_percent_rank",
        "distinct_track_id_in_listener_id",
        "ms_played_avg_7",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.coalesce(F.col("ms_played_sum_3_prior"), F.lit(0)).alias("ms_played_sum_3_prior_filled"),
        F.greatest(F.col("ms_played"), F.col("prev_ms_played")).alias("greatest_ms_played_prev_ms_played"),
    )
)

result = (
    labeled
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played_dense_rank",
        "prev_ms_played",
        "listener_signup_date_percent_rank",
        "distinct_track_id_in_listener_id",
        "ms_played_avg_7",
        "played_at_date",
        "ms_played_sum_3_prior_filled",
        "greatest_ms_played_prev_ms_played",
    )
)
